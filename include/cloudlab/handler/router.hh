#ifndef CLOUDLAB_ROUTER_HH
#define CLOUDLAB_ROUTER_HH

#include "cloudlab/handler/handler.hh"
#include "cloudlab/network/address.hh"
#include "cloudlab/network/routing.hh"

#include <unordered_set>

namespace cloudlab {

/**
 * Handler for the routing tier. Forwards requests to the right peer and handles
 * joining / leaving peers.
 */
class RouterHandler : public ServerHandler {
 public:
  explicit RouterHandler(Routing& routing) : routing{routing} {
    routing.set_partitions_to_cluster_size();
  }

  auto handle_connection(Connection& con) -> void override;

 private:
  auto handle_key_operation(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_join_cluster(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_partitions_added(Connection& con, const cloud::CloudMessage& msg) -> void;
  auto handle_partitions_removed(Connection& con, const cloud::CloudMessage& msg) -> void;

  auto add_new_node(const SocketAddress& peer) -> void;

  auto redistribute_partitions() -> void;

  std::unordered_set<SocketAddress> nodes;

  Routing& routing;
};

}  // namespace cloudlab

#endif  // CLOUDLAB_ROUTER_HH
