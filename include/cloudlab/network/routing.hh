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
    const auto cluster_partitions = 5;

/**
 * Routing class to map keys to peers.
 */
    class Routing {
    public:
        explicit Routing(const std::string &backend_address)
                : backend_address{SocketAddress{backend_address}} {
        }

        auto add_peer(uint32_t partition, const SocketAddress &peer) {
            if (partition >= partitions) return;
            auto search = table.find(partition);
            if (search != table.end()) {
                auto& v = search->second;
                if (std::find(v.begin(), v.end(), peer) == v.end()) {
                    v.emplace_back(peer);
                }
                return;
            } else {
                table.insert({partition, {peer}});
            }
        }

        auto remove_peer(uint32_t partition, const SocketAddress &peer) {
            if (partition >= partitions) return;
            auto search = table.find(partition);
            if (search != table.end()) {
                auto& v = search->second;
                auto vsearch = std::find(v.begin(), v.end(), peer);
                if (vsearch != v.end()) v.erase(vsearch);
            }
        }

        auto find_peer(const std::string &key) -> std::optional<SocketAddress> {
            if (partitions == 0) return {};
            uint64_t part = get_partition(key);
            auto search = table.find(part);
            if (search != table.end()) {
                auto& v = search->second;
                if (v.empty()) return {};
                return {v.at(0)};
            }
            return {};
        }

        auto get_partition(const std::string &key) const -> uint32_t {
            uint64_t part = 0;
            uint64_t multiplier = 1 % cluster_partitions;
            for (uint64_t i = key.length(); i != 0; i--) {
                part = (part + (multiplier * (key.at(i - 1) % cluster_partitions) % cluster_partitions)) % cluster_partitions;
                multiplier = (multiplier * (31 % cluster_partitions)) % cluster_partitions;
            }
            return part;
        }

        auto partitions_by_peer()
        -> std::unordered_map<SocketAddress, std::unordered_set<uint32_t>> {
            std::unordered_map<SocketAddress, std::unordered_set<uint32_t>> sort;
            for (auto &p: table) {
                for (auto &peer: p.second) {
                    auto search = sort.find(peer);
                    if (search != sort.end()) {
                        auto& parts = search->second;
                        parts.insert(p.first);
                    } else {
                        sort.insert({peer, {p.first}});
                    }

                }
            }
            return sort;
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
