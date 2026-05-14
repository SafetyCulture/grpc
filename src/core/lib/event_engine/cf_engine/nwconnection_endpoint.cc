// Copyright 2026 SafetyCulture
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <grpc/support/port_platform.h>

#ifdef GPR_APPLE
#include <AvailabilityMacros.h>
#ifdef AVAILABLE_MAC_OS_X_VERSION_10_14_AND_LATER

#include <cstring>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"

#include "src/core/lib/event_engine/cf_engine/nwconnection_endpoint.h"
#include "src/core/lib/event_engine/trace.h"

namespace grpc_event_engine {
namespace experimental {

namespace {

constexpr int kDefaultReadBufferMax = 8192;
constexpr int kDefaultReadBufferMin = 1;

// Translate an nw_error_t into an absl::Status. nw_error_t is the only error
// channel for Network.framework — the kernel route table / TLS / DNS layers
// all surface failures through nw_error_get_error_code(). We map them all to
// kUnavailable so the gRPC channel can rebuild rather than swallow the call.
absl::Status NWErrorToStatus(nw_error_t error, const char* context) {
  if (error == nullptr) {
    return absl::OkStatus();
  }
  int code = nw_error_get_error_code(error);
  nw_error_domain_t domain = nw_error_get_error_domain(error);
  return absl::UnavailableError(
      absl::StrFormat("nw_connection %s: domain=%d code=%d", context,
                      static_cast<int>(domain), code));
}

// Pull all bytes from a dispatch_data_t into a SliceBuffer using a single
// allocation. Network.framework hands us a (possibly fragmented) dispatch_data
// from nw_connection_receive; gRPC's reader is happiest with one slice.
void AppendDispatchDataToSliceBuffer(dispatch_data_t content,
                                     MemoryAllocator& allocator,
                                     SliceBuffer* buffer) {
  if (content == nullptr || buffer == nullptr) {
    return;
  }
  size_t total = dispatch_data_get_size(content);
  if (total == 0) {
    return;
  }
  auto buffer_index = buffer->AppendIndexed(Slice(allocator.MakeSlice(total)));
  uint8_t* dst = internal::SliceCast<MutableSlice>(
                     buffer->MutableSliceAt(buffer_index))
                     .begin();
  if (dst == nullptr) {
    // Allocator returned a slice without backing storage — extremely unlikely
    // but bail rather than memcpy into nullptr.
    buffer->RemoveLastNBytes(total);
    return;
  }
  __block size_t written = 0;
  dispatch_data_apply(content, ^bool(dispatch_data_t /*region*/, size_t offset,
                                     const void* src, size_t size) {
    (void)offset;
    if (src == nullptr || size == 0) {
      return true;
    }
    memcpy(dst + written, src, size);
    written += size;
    return true;
  });
  // Trim any unused tail in case the allocator gave us slightly more.
  if (written < total) {
    buffer->RemoveLastNBytes(total - written);
  }
}

// Build a contiguous dispatch_data_t covering every slice in `data`. We copy
// here rather than zero-copy because the SliceBuffer's lifetime is shorter
// than the send's; the nw_connection send completion may fire after gRPC has
// already reclaimed those slices. Returns dispatch_data_empty (a singleton)
// rather than null when `data` is null or empty so callers can pass the
// result straight into nw_connection_send.
dispatch_data_t SliceBufferToDispatchData(SliceBuffer* data) {
  if (data == nullptr) {
    return dispatch_data_empty;
  }
  dispatch_data_t result = dispatch_data_empty;
  for (size_t i = 0; i < data->Count(); i++) {
    auto slice = data->RefSlice(i);
    const uint8_t* bytes = slice.begin();
    size_t len = slice.size();
    if (bytes == nullptr || len == 0) {
      // Skip empty / null-backed slices — passing them to dispatch_data_create
      // is well-defined for (NULL, 0) but still unnecessary work.
      continue;
    }
    dispatch_data_t chunk =
        dispatch_data_create(bytes, len,
                             /*queue=*/nullptr,
                             DISPATCH_DATA_DESTRUCTOR_DEFAULT);
    if (chunk == nullptr) continue;
    dispatch_data_t combined = dispatch_data_create_concat(result, chunk);
#if !__has_feature(objc_arc)
    dispatch_release(chunk);
    dispatch_release(result);
#endif
    if (combined == nullptr) {
      // dispatch_data_create_concat shouldn't return null but if it does the
      // chain so far is gone; restart from empty so we don't return null.
      result = dispatch_data_empty;
      continue;
    }
    result = combined;
  }
  return result;
}

}  // namespace

NWConnectionEndpointImpl::NWConnectionEndpointImpl(
    std::shared_ptr<CFEventEngine> engine, MemoryAllocator memory_allocator)
    : engine_(std::move(engine)),
      memory_allocator_(std::move(memory_allocator)) {}

NWConnectionEndpointImpl::~NWConnectionEndpointImpl() {
  if (connection_ != nullptr) {
#if !__has_feature(objc_arc)
    nw_release(connection_);
#endif
    connection_ = nullptr;
  }
}

void NWConnectionEndpointImpl::Shutdown() {
  GRPC_EVENT_ENGINE_ENDPOINT_TRACE(
      "NWConnectionEndpointImpl::Shutdown: this: %p", this);

  {
    grpc_core::MutexLock lock(&connect_mu_);
    shutdown_status_ = absl::UnavailableError("NWConnectionEndpoint shutdown");
  }
  // connection_ is set once in Connect() and only released by the destructor,
  // so the only way it's null here is if Shutdown() raced ahead of Connect()
  // (or Connect() bailed out before nw_connection_create). nw_connection_cancel
  // is documented as a no-op on a cancelled or completed connection but is
  // *not* tolerant of nullptr, so guard explicitly.
  if (connection_ != nullptr) {
    nw_connection_cancel(connection_);
  }
}

bool NWConnectionEndpointImpl::CancelConnect(absl::Status status) {
  GRPC_EVENT_ENGINE_ENDPOINT_TRACE(
      "NWConnectionEndpointImpl::CancelConnect: status: %s, this: %p",
      status.ToString().c_str(), this);

  absl::AnyInvocable<void(absl::Status)> on_connect_to_fire;
  {
    grpc_core::MutexLock lock(&connect_mu_);
    if (connected_ || on_connect_ == nullptr) {
      return false;
    }
    shutdown_status_ = status;
    on_connect_to_fire = std::move(on_connect_);
  }
  if (connection_ != nullptr) {
    nw_connection_cancel(connection_);
  }
  on_connect_to_fire(std::move(status));
  return true;
}

void NWConnectionEndpointImpl::Connect(
    absl::AnyInvocable<void(absl::Status)> on_connect,
    EventEngine::ResolvedAddress addr) {
  auto addr_uri = ResolvedAddressToURI(addr);
  if (!addr_uri.ok()) {
    on_connect(std::move(addr_uri).status());
    return;
  }
  GRPC_EVENT_ENGINE_ENDPOINT_TRACE("NWConnectionEndpointImpl::Connect: %s",
                                   addr_uri.value().c_str());

  peer_address_ = std::move(addr);
  auto host_port = ResolvedAddressToNormalizedString(peer_address_);
  if (!host_port.ok()) {
    on_connect(std::move(host_port).status());
    return;
  }
  peer_address_string_ = host_port.value();

  // ResolvedAddress always carries a valid sockaddr after DNS resolution, but
  // guard against a malformed input rather than passing nullptr to
  // nw_endpoint_create_address (which is undefined behaviour).
  const struct sockaddr* peer_sockaddr = peer_address_.address();
  if (peer_sockaddr == nullptr) {
    on_connect(absl::InvalidArgumentError(absl::StrCat(
        "NWConnectionEndpoint::Connect: peer address has null sockaddr for ",
        peer_address_string_)));
    return;
  }

  // Build an nw_endpoint directly from the resolved sockaddr (we already did
  // DNS in the cf_engine's dns_service_resolver — no need to re-resolve).
  nw_endpoint_t endpoint = nw_endpoint_create_address(peer_sockaddr);
  if (endpoint == nullptr) {
    on_connect(absl::InvalidArgumentError(
        absl::StrCat("nw_endpoint_create_address failed for ",
                     peer_address_string_)));
    return;
  }

  // Raw TCP — TLS stays in BoringSSL on top of us. nw_parameters_create_secure_tcp
  // is not documented to return null but the dispatch_data_create-style APIs
  // in Network.framework can return null under extreme memory pressure; guard
  // and release the partially-built endpoint to avoid a leak.
  nw_parameters_t parameters = nw_parameters_create_secure_tcp(
      NW_PARAMETERS_DISABLE_PROTOCOL, NW_PARAMETERS_DEFAULT_CONFIGURATION);
  if (parameters == nullptr) {
#if !__has_feature(objc_arc)
    nw_release(endpoint);
#endif
    on_connect(absl::UnavailableError(
        "nw_parameters_create_secure_tcp returned null"));
    return;
  }

  connection_ = nw_connection_create(endpoint, parameters);
#if !__has_feature(objc_arc)
  nw_release(endpoint);
  nw_release(parameters);
#endif

  if (connection_ == nullptr) {
    on_connect(absl::UnavailableError("nw_connection_create returned null"));
    return;
  }

  {
    grpc_core::MutexLock lock(&connect_mu_);
    on_connect_ = std::move(on_connect);
  }

  auto self_ref = Ref();
  nw_connection_set_state_changed_handler(
      connection_, ^(nw_connection_state_t state, nw_error_t error) {
        self_ref->OnStateChanged(state, error);
      });
  nw_connection_set_queue(connection_, DefaultQueue());
  nw_connection_start(connection_);
}

void NWConnectionEndpointImpl::OnStateChanged(nw_connection_state_t state,
                                              nw_error_t error) {
  GRPC_EVENT_ENGINE_ENDPOINT_TRACE(
      "NWConnectionEndpointImpl::OnStateChanged: state=%d this=%p",
      static_cast<int>(state), this);

  switch (state) {
    case nw_connection_state_waiting:
    case nw_connection_state_preparing:
      // Transient. Both fire while Network.framework is selecting a path
      // (DNS, route, interface). Every cold-start passes through `waiting`
      // briefly; cancelling here would kill the very first connect attempt
      // and trap the channel in a retry loop. Fast-fail is provided by
      // gRPC's connect-deadline timer (see cf_engine.cc::Connect) which
      // routes through CancelConnect → `cancelled` state below.
      return;
    case nw_connection_state_ready: {
      PopulateLocalAddress();
      absl::AnyInvocable<void(absl::Status)> cb;
      {
        grpc_core::MutexLock lock(&connect_mu_);
        if (on_connect_ == nullptr) return;
        connected_ = true;
        cb = std::move(on_connect_);
      }
      cb(absl::OkStatus());
      return;
    }
    case nw_connection_state_failed: {
      absl::Status status = NWErrorToStatus(error, "failed");
      if (status.ok()) {
        status = absl::UnavailableError("nw_connection failed");
      }
      absl::AnyInvocable<void(absl::Status)> cb;
      {
        grpc_core::MutexLock lock(&connect_mu_);
        if (on_connect_ != nullptr) {
          cb = std::move(on_connect_);
        }
        shutdown_status_ = status;
      }
      if (cb) {
        cb(std::move(status));
      }
      return;
    }
    case nw_connection_state_cancelled: {
      absl::AnyInvocable<void(absl::Status)> cb;
      {
        grpc_core::MutexLock lock(&connect_mu_);
        if (on_connect_ != nullptr) {
          cb = std::move(on_connect_);
        }
      }
      if (cb) {
        cb(absl::CancelledError("nw_connection cancelled"));
      }
      return;
    }
    case nw_connection_state_invalid:
    default:
      return;
  }
}

void NWConnectionEndpointImpl::PopulateLocalAddress() {
  // Called only from the `ready` state, so connection_ should never be null
  // here — but the cost of being wrong is undefined behaviour inside
  // nw_connection_copy_current_path, so guard explicitly.
  if (connection_ == nullptr) return;
  // Try to fish the effective local endpoint out of the path. If the path
  // type isn't an address endpoint (e.g. happy-eyeballs collapsed to a Bonjour
  // service name), leave local_address_ as default — gRPC only uses it for
  // logging/channelz on the client side.
  nw_path_t path = nw_connection_copy_current_path(connection_);
  if (path == nullptr) return;
  nw_endpoint_t local_ep = nw_path_copy_effective_local_endpoint(path);
#if !__has_feature(objc_arc)
  nw_release(path);
#endif
  if (local_ep == nullptr) return;
  if (nw_endpoint_get_type(local_ep) == nw_endpoint_type_address) {
    const struct sockaddr* sa = nw_endpoint_get_address(local_ep);
    if (sa != nullptr) {
      socklen_t len = (sa->sa_family == AF_INET6)
                          ? sizeof(struct sockaddr_in6)
                          : sizeof(struct sockaddr_in);
      local_address_ = EventEngine::ResolvedAddress(sa, len);
      auto uri = ResolvedAddressToURI(local_address_);
      if (uri.ok()) local_address_string_ = uri.value();
    }
  }
#if !__has_feature(objc_arc)
  nw_release(local_ep);
#endif
}

bool NWConnectionEndpointImpl::Read(
    absl::AnyInvocable<void(absl::Status)> on_read, SliceBuffer* buffer,
    const EventEngine::Endpoint::ReadArgs* /* args */) {
  GRPC_EVENT_ENGINE_ENDPOINT_TRACE("NWConnectionEndpointImpl::Read, this: %p",
                                   this);

  // Defensive: gRPC's chttp2 transport supplies a non-null buffer per the
  // EventEngine::Endpoint contract, but a null connection_ means Read was
  // called before Connect() succeeded (or after it failed). Fire the callback
  // synchronously so gRPC doesn't sit waiting on a closure that will never
  // arrive.
  if (buffer == nullptr) {
    on_read(absl::InvalidArgumentError(
        "NWConnectionEndpointImpl::Read: null buffer"));
    return false;
  }
  if (connection_ == nullptr) {
    on_read(absl::UnavailableError(
        "NWConnectionEndpointImpl::Read: connection not established"));
    return false;
  }

  auto self_ref = Ref();
  // `on_read` is a move-only AnyInvocable — wrap it in a shared_ptr so the
  // ObjC block can capture it by value.
  auto callback =
      std::make_shared<absl::AnyInvocable<void(absl::Status)>>(std::move(on_read));

  nw_connection_receive(
      connection_, kDefaultReadBufferMin, kDefaultReadBufferMax,
      ^(dispatch_data_t content, nw_content_context_t /*ctx*/,
        bool is_complete, nw_error_t error) {
        if (error != nullptr) {
          (*callback)(NWErrorToStatus(error, "receive"));
          return;
        }
        if (content != nullptr) {
          AppendDispatchDataToSliceBuffer(content, self_ref->memory_allocator_,
                                          buffer);
        }
        if (is_complete && (content == nullptr ||
                            dispatch_data_get_size(content) == 0)) {
          (*callback)(absl::UnavailableError("nw_connection EOF"));
          return;
        }
        (*callback)(absl::OkStatus());
      });

  // Always asynchronous — nw_connection_receive never completes inline.
  return false;
}

bool NWConnectionEndpointImpl::Write(
    absl::AnyInvocable<void(absl::Status)> on_writable, SliceBuffer* data,
    const EventEngine::Endpoint::WriteArgs* /* args */) {
  GRPC_EVENT_ENGINE_ENDPOINT_TRACE("NWConnectionEndpointImpl::Write, this: %p",
                                   this);

  if (data == nullptr) {
    on_writable(absl::InvalidArgumentError(
        "NWConnectionEndpointImpl::Write: null data"));
    return false;
  }
  if (connection_ == nullptr) {
    on_writable(absl::UnavailableError(
        "NWConnectionEndpointImpl::Write: connection not established"));
    return false;
  }

  dispatch_data_t payload = SliceBufferToDispatchData(data);
  if (payload == nullptr) {
    // SliceBufferToDispatchData returns dispatch_data_empty (a singleton) when
    // data is empty, so a true null indicates allocation failure inside
    // libdispatch. Surface as Unavailable so gRPC can retry the call.
    on_writable(absl::UnavailableError(
        "NWConnectionEndpointImpl::Write: failed to build dispatch_data"));
    return false;
  }

  auto self_ref = Ref();
  auto callback = std::make_shared<absl::AnyInvocable<void(absl::Status)>>(
      std::move(on_writable));

  nw_connection_send(connection_, payload,
                     NW_CONNECTION_DEFAULT_MESSAGE_CONTEXT,
                     /*is_complete=*/true, ^(nw_error_t error) {
                       // Explicit reference so the ObjC block captures
                       // `self_ref` by value, keeping the impl alive until the
                       // send completion fires. Without this the block only
                       // captures `callback` and the connection_ field could be
                       // released mid-completion if the endpoint is destroyed
                       // while a send is in flight.
                       (void)self_ref;
                       if (error != nullptr) {
                         (*callback)(NWErrorToStatus(error, "send"));
                         return;
                       }
                       (*callback)(absl::OkStatus());
                     });

#if !__has_feature(objc_arc)
  dispatch_release(payload);
#endif
  return false;
}

}  // namespace experimental
}  // namespace grpc_event_engine

#endif  // AVAILABLE_MAC_OS_X_VERSION_10_14_AND_LATER
#endif  // GPR_APPLE
