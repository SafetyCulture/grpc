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

// Endpoint implementation backed by Apple's Network.framework
// (nw_connection_t). Drop-in replacement for CFStreamEndpoint inside
// cf_engine. Runs raw TCP only — BoringSSL handles TLS on top.
//
// Why this exists: CFStream's CFWriteStreamWrite can accept bytes that
// never reach the wire after a network transition, leaving the channel
// silently half-open. nw_connection cooperates with the kernel route
// table and surfaces such failures as connection-state transitions.

#ifndef GRPC_SRC_CORE_LIB_EVENT_ENGINE_CF_ENGINE_NWCONNECTION_ENDPOINT_H
#define GRPC_SRC_CORE_LIB_EVENT_ENGINE_CF_ENGINE_NWCONNECTION_ENDPOINT_H
#include <grpc/support/port_platform.h>

#ifdef GPR_APPLE
#include <AvailabilityMacros.h>
#ifdef AVAILABLE_MAC_OS_X_VERSION_10_14_AND_LATER

#include <Network/Network.h>

#include "absl/strings/str_format.h"

#include <grpc/event_engine/event_engine.h>

#include "src/core/lib/address_utils/sockaddr_utils.h"
#include "src/core/lib/event_engine/cf_engine/cf_engine.h"
#include "src/core/lib/event_engine/tcp_socket_utils.h"
#include "src/core/lib/gprpp/host_port.h"
#include "src/core/lib/gprpp/ref_counted.h"
#include "src/core/lib/gprpp/ref_counted_ptr.h"

namespace grpc_event_engine {
namespace experimental {

class NWConnectionEndpointImpl
    : public grpc_core::RefCounted<NWConnectionEndpointImpl> {
 public:
  NWConnectionEndpointImpl(std::shared_ptr<CFEventEngine> engine,
                           MemoryAllocator memory_allocator);
  ~NWConnectionEndpointImpl();

  void Shutdown();

  bool Read(absl::AnyInvocable<void(absl::Status)> on_read, SliceBuffer* buffer,
            const EventEngine::Endpoint::ReadArgs* args);
  bool Write(absl::AnyInvocable<void(absl::Status)> on_writable,
             SliceBuffer* data, const EventEngine::Endpoint::WriteArgs* args);

  const EventEngine::ResolvedAddress& GetPeerAddress() const {
    return peer_address_;
  }
  const EventEngine::ResolvedAddress& GetLocalAddress() const {
    return local_address_;
  }

  void Connect(absl::AnyInvocable<void(absl::Status)> on_connect,
               EventEngine::ResolvedAddress addr);
  bool CancelConnect(absl::Status status);

 private:
  void OnStateChanged(nw_connection_state_t state, nw_error_t error);
  void PopulateLocalAddress();

  // Owned dispatch queue for the connection's callbacks. A global concurrent
  // queue is fine — we never serialize on it ourselves; ordering is enforced
  // by gRPC's outer layers.
  static dispatch_queue_t DefaultQueue() {
    return dispatch_get_global_queue(QOS_CLASS_DEFAULT, 0);
  }

  nw_connection_t connection_ = nullptr;

  std::shared_ptr<CFEventEngine> engine_;

  EventEngine::ResolvedAddress peer_address_;
  EventEngine::ResolvedAddress local_address_;
  std::string peer_address_string_;
  std::string local_address_string_;
  MemoryAllocator memory_allocator_;

  // Guards on_connect_ during the connect handshake. Once nw_connection
  // reaches `ready` or `failed`, the closure is consumed and cleared.
  grpc_core::Mutex connect_mu_;
  absl::AnyInvocable<void(absl::Status)> on_connect_
      ABSL_GUARDED_BY(connect_mu_);
  bool connected_ ABSL_GUARDED_BY(connect_mu_) = false;
  absl::Status shutdown_status_ ABSL_GUARDED_BY(connect_mu_) = absl::OkStatus();
};

class NWConnectionEndpoint : public EventEngine::Endpoint {
 public:
  NWConnectionEndpoint(std::shared_ptr<CFEventEngine> engine,
                       MemoryAllocator memory_allocator) {
    impl_ = grpc_core::MakeRefCounted<NWConnectionEndpointImpl>(
        std::move(engine), std::move(memory_allocator));
  }
  ~NWConnectionEndpoint() override { impl_->Shutdown(); }

  bool Read(absl::AnyInvocable<void(absl::Status)> on_read, SliceBuffer* buffer,
            const ReadArgs* args) override {
    return impl_->Read(std::move(on_read), buffer, args);
  }
  bool Write(absl::AnyInvocable<void(absl::Status)> on_writable,
             SliceBuffer* data, const WriteArgs* args) override {
    return impl_->Write(std::move(on_writable), data, args);
  }

  const EventEngine::ResolvedAddress& GetPeerAddress() const override {
    return impl_->GetPeerAddress();
  }
  const EventEngine::ResolvedAddress& GetLocalAddress() const override {
    return impl_->GetLocalAddress();
  }

  void Connect(absl::AnyInvocable<void(absl::Status)> on_connect,
               EventEngine::ResolvedAddress addr) {
    impl_->Connect(std::move(on_connect), std::move(addr));
  }
  bool CancelConnect(absl::Status status) {
    return impl_->CancelConnect(std::move(status));
  }

 private:
  grpc_core::RefCountedPtr<NWConnectionEndpointImpl> impl_;
};

}  // namespace experimental
}  // namespace grpc_event_engine

#endif  // AVAILABLE_MAC_OS_X_VERSION_10_14_AND_LATER
#endif  // GPR_APPLE

#endif  // GRPC_SRC_CORE_LIB_EVENT_ENGINE_CF_ENGINE_NWCONNECTION_ENDPOINT_H
