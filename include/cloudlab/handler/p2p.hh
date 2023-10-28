#ifndef CLOUDLAB_P2P_HH
#define CLOUDLAB_P2P_HH

#include "cloudlab/handler/handler.hh"
#include "cloudlab/kvs.hh"
#include "cloudlab/network/routing.hh"

namespace cloudlab {

/**
 * Handler for P2P requests. Takes care of the messages from peers, cluster
 * metadata / routing tier, and the API.
 */
class P2PHandler : public ServerHandler {
 public:
  explicit P2PHandler(Routing& routing);

  auto handle_connection(Connection& con) -> void override;

 private:
  auto handle_put(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_get(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_delete(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_join_cluster(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_create_partitions(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_steal_partitions(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_drop_partitions(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_transfer_partition(Connection& con, const cloud::CloudMessage& msg) -> void;

  // partitions stored on this peer: [partition ID -> KVS]
  std::unordered_map<uint32_t, std::unique_ptr<KVS>> partitions{};

  Routing& routing;
};

}  // namespace cloudlab

#endif  // CLOUDLAB_P2P_HH
