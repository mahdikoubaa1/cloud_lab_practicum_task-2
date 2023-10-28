#ifndef CLOUDLAB_ROUTING_HH
#define CLOUDLAB_ROUTING_HH

#include "cloudlab/kvs.hh"
#include "cloudlab/network/address.hh"
#include <optional>

#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace cloudlab {

// actually 840 is a good number
const auto cluster_partitions = 4;

/**
 * Routing class to map keys to peers.
 */
class Routing {
 public:
  explicit Routing(const std::string& backend_address)
      : backend_address{SocketAddress{backend_address}} {
  }

  auto add_peer(uint32_t partition, const SocketAddress& peer) {
    // TODO (you)
  }

  auto remove_peer(uint32_t partition, const SocketAddress& peer) {
    // TODO (you)
  }

  auto find_peer(const std::string& key) -> std::optional<SocketAddress> {
    // TODO (you)
  }

  auto get_partition(const std::string& key) const -> uint32_t {
    // TODO (you)
  }

  auto partitions_by_peer()
      -> std::unordered_map<SocketAddress, std::unordered_set<uint32_t>> {
    // TODO (you)
  }

  auto get_cluster_address() -> std::optional<SocketAddress> {
    return cluster_address;
  }

  auto set_cluster_address(std::optional<SocketAddress> address) -> void {
    cluster_address = std::move(address);
  }

  auto get_backend_address() -> SocketAddress {
    return backend_address;
  }

  auto set_partitions_to_cluster_size() -> void {
    partitions = cluster_partitions;
  }

 private:
  size_t partitions{1};

  std::unordered_map<uint32_t, std::vector<SocketAddress>> table;

  // API requests are forwarded to this address
  const SocketAddress backend_address;

  // cluster metadata store, e.g., routing tier
  std::optional<SocketAddress> cluster_address{};
};

}  // namespace cloudlab

#endif  // CLOUDLAB_ROUTING_HH
