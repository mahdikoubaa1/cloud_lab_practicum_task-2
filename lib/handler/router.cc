#include "cloudlab/handler/router.hh"

#include "fmt/core.h"

#include "cloud.pb.h"

namespace cloudlab {
    auto sigpipehandler(int s) -> void {
    }

    auto RouterHandler::handle_connection(Connection &con) -> void {
        cloud::CloudMessage request{}, response{};

        if (!con.receive(request)) {
            return;
        }

        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(request.operation());

        switch (request.operation()) {
            case cloud::CloudMessage_Operation_PUT:
            case cloud::CloudMessage_Operation_GET:
            case cloud::CloudMessage_Operation_DELETE: {
                handle_key_operation(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_JOIN_CLUSTER: {
                handle_join_cluster(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_PARTITIONS_ADDED: {
                handle_partitions_added(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_PARTITIONS_REMOVED: {
                handle_partitions_removed(con, request);
                break;
            }
            default:
                break;
        }
    }

    auto RouterHandler::handle_key_operation(Connection &con,
                                             const cloud::CloudMessage &msg)
    -> void {
        signal(SIGPIPE, sigpipehandler);
        cloud::CloudMessage response;
        response.set_operation(msg.operation());
        response.set_success(true);
        response.set_message("OK");
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        std::unordered_map<SocketAddress, std::pair<std::unique_ptr<Connection>, std::unique_ptr<cloud::CloudMessage>>> tosend;
        for (auto &kvp: msg.kvp()) {
            auto h = routing.find_peer(kvp.key());
            if (!h.has_value()) continue;
            auto x = tosend.find(h.value());
            if (x == tosend.end()) {
                std::pair p{std::make_unique<Connection>(h.value()), std::make_unique<cloud::CloudMessage>()};
                p.second->set_operation(msg.operation());
                p.second->set_type(cloud::CloudMessage_Type_REQUEST);
                auto tmp = p.second->add_kvp();
                tmp->set_key(kvp.key());
                tmp->set_value(kvp.value());
                tosend.insert({h.value(), std::move(p)});
            } else {
                auto tmp = x->second.second->add_kvp();
                tmp->set_value(kvp.value());
                tmp->set_key(kvp.key());
            }
        }
        for (auto &sendpair: tosend) {
            if (!sendpair.second.first->send(*sendpair.second.second)) {
                for (auto &kvp: sendpair.second.second->kvp()) {
                    auto tmp = response.add_kvp();
                    tmp->set_key(kvp.key());
                    tmp->set_value("ERROR");
                }
            }
        }
        for (auto &r: tosend) {
            r.second.first->receive(*r.second.second);
            if (!r.second.second->success() && msg.operation() == cloud::CloudMessage_Operation_PUT) {
                response.set_success(false);
                response.set_message("ERROR");
            }
            for (auto &kvp: r.second.second->kvp()) {
                auto tmp = response.add_kvp();
                tmp->set_key(kvp.key());
                tmp->set_value(kvp.value());
            }
        }
        con.send(response);
    }

    auto RouterHandler::handle_join_cluster(Connection &con,
                                            const cloud::CloudMessage &msg)
    -> void {
        signal(SIGPIPE, sigpipehandler);
        cloud::CloudMessage response;
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_JOIN_CLUSTER);

        cloud::CloudMessage requesttonode;
        requesttonode.set_operation(cloud::CloudMessage_Operation_JOIN_CLUSTER);
        requesttonode.set_type(cloud::CloudMessage_Type_REQUEST);
        auto address = requesttonode.mutable_address();
        address->set_address(msg.address().address());
        Connection con1{msg.address().address()};
        con1.send(requesttonode);
        cloud::CloudMessage responsefromnode;
        con1.receive(responsefromnode);
        response.set_success(responsefromnode.success());
        response.set_message(responsefromnode.message());
        add_new_node(SocketAddress(msg.address().address()));
        std::unordered_map<SocketAddress, std::pair<std::unique_ptr<Connection>, std::unique_ptr<cloud::CloudMessage>>> tosend;
        for (auto &kvp: responsefromnode.kvp()) {
            auto h = routing.find_peer(kvp.key());
            if (!h.has_value()) continue;
            auto x = tosend.find(h.value());
            if (x == tosend.end()) {
                std::pair p{std::make_unique<Connection>(h.value()), std::make_unique<cloud::CloudMessage>()};
                p.second->set_operation(cloud::CloudMessage_Operation_PUT);
                p.second->set_type(cloud::CloudMessage_Type_REQUEST);
                auto tmp = p.second->add_kvp();
                tmp->set_key(kvp.key());
                tmp->set_value(kvp.value());
                tosend.insert({h.value(), std::move(p)});
            } else {
                auto tmp = x->second.second->add_kvp();
                tmp->set_value(kvp.value());
                tmp->set_key(kvp.key());
            }
        }
        for (auto &sendpair: tosend) {
            if (!sendpair.second.first->send(*sendpair.second.second)) {
                for (auto &kvp: sendpair.second.second->kvp()) {
                    auto tmp = response.add_kvp();
                    tmp->set_key(kvp.key());
                    tmp->set_value("ERROR");
                }
            }
        }
        for (auto &r: tosend) {
            r.second.first->receive(*r.second.second);
            if (!r.second.second->success() && msg.operation() == cloud::CloudMessage_Operation_PUT) {
                response.set_success(false);
                response.set_message("ERROR");
            }
            for (auto &kvp: r.second.second->kvp()) {
                auto tmp = response.add_kvp();
                tmp->set_key(kvp.key());
                tmp->set_value(kvp.value());
            }
        }
        con.send(response);
        auto s = routing.partitions_by_peer();
    }

    auto RouterHandler::add_new_node(const SocketAddress &peer) -> void {
        nodes.insert(peer);
        redistribute_partitions();
    }

    auto RouterHandler::redistribute_partitions() -> void {
        std::unordered_map<SocketAddress, std::pair<std::unique_ptr<Connection>, std::unique_ptr<cloud::CloudMessage>>> tosend;
        auto nodespartitions = routing.partitions_by_peer();
        cloud::CloudMessage tester;
        for (auto it = nodes.begin();it!=nodes.end();){
            Connection con { *it};
            if (!con.send(tester)){
                auto search = nodespartitions.find(*it);
                if(search!=nodespartitions.end()){
                    for (auto part : search->second){
                        routing.remove_peer(part,*it);
                    }
                }
                nodespartitions.erase(search);
                it = nodes.erase(it);
                continue;
            }else if (!nodespartitions.contains(*it)) {
                nodespartitions.insert({*it, {}});
            }
            ++it;
        }
        uint32_t numclusters = nodes.size();
        uint32_t div = (cluster_partitions) / numclusters;
        uint32_t mod = (cluster_partitions) % numclusters;
        std::unordered_set<uint32_t> unassociated;
        for (uint32_t i = 0; i < cluster_partitions; i++) {
            unassociated.insert(i);
        }

        auto it = std::find_if(nodespartitions.begin(), nodespartitions.end(), [div](
                std::pair<const SocketAddress, std::unordered_set<unsigned int>> &p) {
            return p.second.size() <= div;
        });
        auto it1 = nodespartitions.begin();
        uint32_t k1;
        uint32_t k;
        uint32_t totrans;
        bool take = true;
        std::unordered_set<uint32_t>::iterator iter1;
        if (it != nodespartitions.end()) {
            k1 = it1->second.size();
            k = it->second.size();
        }
        while (it != nodespartitions.end() && it1 != nodespartitions.end()) {
            if (take) {
                for (auto &part: it1->second) {
                    unassociated.erase(part);
                }
                take = false;
                if (it1->second.size() <= div) {
                    ++it1;
                    if (it1 != nodespartitions.end()) k1 = it1->second.size();
                    take = true;
                    continue;
                }
                iter1 = it1->second.begin();
                std::pair p{std::make_unique<Connection>(it->first), std::make_unique<cloud::CloudMessage>()};
                p.second->set_operation(cloud::CloudMessage_Operation_STEAL_PARTITIONS);
                p.second->set_type(cloud::CloudMessage_Type_REQUEST);
                auto address = p.second->mutable_address();
                address->set_address(it->first.string());
                tosend.insert({it->first, std::move(p)});

            }

            totrans =
                    mod != 0 ? (mod > 1 ? std::min((k1 - div - 1), (div + 1 - k)) : std::min((k1 - div - 1), (div - k)))
                             : std::min((k1 - div), (div - k));
            auto nodeentry = tosend.find(it->first);
            for (; iter1 != it1->second.end() && totrans != 0; ++iter1) {
                auto tmp = nodeentry->second.second->add_partition();
                tmp->set_id(*iter1);
                tmp->set_peer(it1->first.string());
                --totrans;
                ++k;
                --k1;
            }
            if (mod != 0) {
                if (mod > 1) {

                    if (k == div + 1) {
                        it = std::find_if(++it, nodespartitions.end(), [div](
                                std::pair<const SocketAddress, std::unordered_set<unsigned int>> &p) {
                            return p.second.size() <= div;
                        });
                        --mod;
                        if (it != nodespartitions.end()) {
                            k = it->second.size();
                        }
                    }
                } else {

                    if (k == div) {
                        it = std::find_if(++it, nodespartitions.end(), [div](
                                std::pair<const SocketAddress, std::unordered_set<unsigned int>> &p) {
                            return p.second.size() <= div;
                        });
                        if (it != nodespartitions.end()) {
                            k = it->second.size();

                        }
                    }

                }
                if (k1 == div + 1) {
                    ++it1;
                    --mod;
                    take = true;
                    if (it1 != nodespartitions.end()) k1 = it1->second.size();
                }
            } else {
                if (k == div) {
                    it = std::find_if(++it, nodespartitions.end(), [div](
                            std::pair<const SocketAddress, std::unordered_set<unsigned int>> &p) {
                        return p.second.size() <= div;
                    });
                    if (it != nodespartitions.end()) {
                        k = it->second.size();

                    }
                }
                if (k1 == div) {
                    ++it1;
                    take = true;
                    if (it1 != nodespartitions.end()) k1 = it1->second.size();
                }
            }
        }
        for (auto &sender: tosend) {
            sender.second.first->send(*sender.second.second);
        }
        for (auto &receiver: tosend) {
            receiver.second.first->receive(*receiver.second.second);
        }

        auto iter2 = unassociated.begin();
        std::vector<std::pair<std::unique_ptr<Connection>, std::unique_ptr<cloud::CloudMessage>>> tosendcreate;
        while (it != nodespartitions.end()) {
            std::pair p{std::make_unique<Connection>(it->first), std::make_unique<cloud::CloudMessage>()};
            p.second->set_operation(cloud::CloudMessage_Operation_CREATE_PARTITIONS);
            p.second->set_type(cloud::CloudMessage_Type_REQUEST);
            auto address = p.second->mutable_address();
            address->set_address(it->first.string());
            for (; iter2 != unassociated.end() && k < (mod > 0 ? div + 1 : div);) {
                auto tmp = p.second->add_partition();
                tmp->set_id(*iter2);
                iter2 = unassociated.erase(iter2); //badelha
                ++k;
            }
            p.first->send(*p.second);
            tosendcreate.push_back(std::move(p));
            if (mod > 0) --mod;
            it = std::find_if(++it, nodespartitions.end(), [div](
                    std::pair<const SocketAddress, std::unordered_set<unsigned int>> &p) {
                return p.second.size() <= div;
            });
            if (it != nodespartitions.end()) { k = it->second.size(); }

        }
        for (auto &receiver: tosendcreate) {
            receiver.first->receive(*receiver.second);
        }

    }

    auto RouterHandler::handle_partitions_added(Connection &con,
                                                const cloud::CloudMessage &msg)
    -> void {
        if (msg.success()) {
            for (auto &p: msg.partition()) {
                routing.add_peer(p.id(), SocketAddress(p.peer()));
            }

        }
        cloud::CloudMessage response;
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_PARTITIONS_ADDED);
        response.set_message(msg.message());
        response.set_success(msg.success());
        con.send(response);

    }

    auto RouterHandler::handle_partitions_removed(Connection &con,
                                                  const cloud::CloudMessage &msg)
    -> void {
        if (msg.success()) {
            for (auto &p: msg.partition()) {
                routing.remove_peer(p.id(), SocketAddress(p.peer()));
            }
        }
        cloud::CloudMessage response;
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_PARTITIONS_ADDED);
        response.set_message(msg.message());
        response.set_success(msg.success());
        con.send(response);
    }

}  // namespace cloudlab