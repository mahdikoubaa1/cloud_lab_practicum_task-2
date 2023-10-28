#include "cloudlab/handler/p2p.hh"

#include "fmt/core.h"

#include "cloud.pb.h"

namespace cloudlab {

    P2PHandler::P2PHandler(Routing &routing) : routing{routing} {
        auto hash = std::hash<SocketAddress>()(routing.get_backend_address());
        auto path = fmt::format("/tmp/{}-initial", hash);
        partitions.insert({0, std::make_unique<KVS>(path)});
    }

    auto P2PHandler::handle_connection(Connection &con) -> void {
        cloud::CloudMessage request{}, response{};

        if (!con.receive(request)) {
            return;
        }

        if (request.type() != cloud::CloudMessage_Type_REQUEST) {
            throw std::runtime_error("p2p.cc: expected a request");
        }

        switch (request.operation()) {
            case cloud::CloudMessage_Operation_PUT: {
                handle_put(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_GET: {
                handle_get(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_DELETE: {
                handle_delete(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_JOIN_CLUSTER: {
                handle_join_cluster(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_CREATE_PARTITIONS: {
                handle_create_partitions(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_STEAL_PARTITIONS: {
                handle_steal_partitions(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_DROP_PARTITIONS: {
                handle_drop_partitions(con, request);
                break;
            }
            case cloud::CloudMessage_Operation_TRANSFER_PARTITION: {
                handle_transfer_partition(con, request);
                break;
            }
            default:
                response.set_type(cloud::CloudMessage_Type_RESPONSE);
                response.set_operation(request.operation());
                response.set_success(false);
                response.set_message("Operation not (yet) supported");

                con.send(response);

                break;
        }
    }

    auto P2PHandler::handle_put(Connection &con, const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response;
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_PUT);
        response.set_success(true);
        response.set_message("OK");

        for (const auto &kvp: msg.kvp()) {
            auto *tmp = response.add_kvp();
            tmp->set_key(kvp.key());
            auto search = partitions.find(routing.get_partition(kvp.key()));
            if (search == partitions.end() || !search->second) {
                tmp->set_value("ERROR");
                response.set_success(false);
                response.set_message("ERROR");
                continue;
            }

            if (search->second->put(kvp.key(), kvp.value())) {
                tmp->set_value("OK");
            } else {
                tmp->set_value("ERROR");
                response.set_success(false);
                response.set_message("ERROR");
            }
        }

        con.send(response);
    }

    auto P2PHandler::handle_get(Connection &con, const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response{};
        std::string value;

        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_GET);
        response.set_success(true);
        response.set_message("OK");

        for (const auto &kvp: msg.kvp()) {

            auto *tmp = response.add_kvp();
            tmp->set_key(kvp.key());
            auto search = partitions.find(routing.get_partition(kvp.key()));
            if (search == partitions.end() || !search->second) {
                tmp->set_value("ERROR");
                continue;
            }
            if (search->second->get(kvp.key(), value)) {
                tmp->set_value(value);
            } else {
                tmp->set_value("ERROR");
            }
        }

        con.send(response);
    }

    auto P2PHandler::handle_delete(Connection &con, const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response{};
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_DELETE);
        response.set_success(true);
        response.set_message("OK");

        for (const auto &kvp: msg.kvp()) {
            auto *tmp = response.add_kvp();
            tmp->set_key(kvp.key());
            auto search = partitions.find(routing.get_partition(kvp.key()));
            if (search == partitions.end() || !search->second) {
                tmp->set_value("ERROR");
                continue;
            }
            if (search->second->remove(kvp.key())) {
                tmp->set_value("OK");
            } else {
                tmp->set_value("ERROR");
            }
        }
        con.send(response);
    }

    auto P2PHandler::handle_join_cluster(Connection &con,
                                         const cloud::CloudMessage &msg) -> void {
        cloud::CloudMessage response;
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_JOIN_CLUSTER);
        response.set_message("OK");
        response.set_success(true);
        cloud::CloudMessage requestresponse;
        requestresponse.set_type(cloud::CloudMessage_Type_REQUEST);
        requestresponse.set_operation(cloud::CloudMessage_Operation_PARTITIONS_REMOVED);
        requestresponse.set_message("OK");
        requestresponse.set_success(true);

        std::vector<std::pair<std::string, std::string>> keyvalues;
        for (auto it = partitions.begin(); it != partitions.end();) {
            if (it->second) {
                if (!it->second->get_all(keyvalues)) {
                    response.set_success(false);
                    response.set_message("ERROR");

                }
                it->second->clear();

            }
            auto tmp = requestresponse.add_partition();
            tmp->set_peer(msg.address().address());
            tmp->set_id(it->first);
            it = partitions.erase(it);
        }
        for (auto &keyvalue: keyvalues) {
            auto tmp = response.add_kvp();
            tmp->set_value(keyvalue.second);
            tmp->set_key(keyvalue.first);
        }
        Connection con1(routing.get_cluster_address().value());
        con1.send(requestresponse);
        con1.receive(requestresponse);
        response.set_message(requestresponse.message());
        response.set_success(requestresponse.success());
        con.send(response);

    }

    auto P2PHandler::handle_create_partitions(Connection &con,
                                              const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response;
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_CREATE_PARTITIONS);
        cloud::CloudMessage requestresponse;
        requestresponse.set_type(cloud::CloudMessage_Type_REQUEST);
        requestresponse.set_operation(cloud::CloudMessage_Operation_PARTITIONS_ADDED);
        requestresponse.set_message("OK");
        requestresponse.set_success(true);
        for (auto &part: msg.partition()) {
            auto path = fmt::format("/tmp/{}", part.id());
            partitions.insert({part.id(), std::make_unique<KVS>(path)});
            auto tmp = requestresponse.add_partition();
            tmp->set_peer(msg.address().address());
            tmp->set_id(part.id());
        }
        Connection con1{routing.get_cluster_address().value()};
        con1.send(requestresponse);
        con1.receive(requestresponse);
        response.set_message(requestresponse.message());
        response.set_success(requestresponse.success());
        con.send(response);
    }

    auto P2PHandler::handle_steal_partitions(Connection &con,
                                             const cloud::CloudMessage &msg)
    -> void {
        std::unordered_map<SocketAddress, std::pair<std::unique_ptr<Connection>, std::unique_ptr<cloud::CloudMessage>>> tosend;
        cloud::CloudMessage response;
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_STEAL_PARTITIONS);
        cloud::CloudMessage requestresponse;
        requestresponse.set_type(cloud::CloudMessage_Type_REQUEST);
        requestresponse.set_operation(cloud::CloudMessage_Operation_PARTITIONS_ADDED);
        requestresponse.set_message("OK");
        requestresponse.set_success(true);

        for (auto &part: msg.partition()) {
            auto path = fmt::format("/tmp/{}", part.id());
            partitions.insert({part.id(), std::make_unique<KVS>(path)});
            auto tmp = requestresponse.add_partition();
            tmp->set_peer(msg.address().address());
            tmp->set_id(part.id());
            auto x = tosend.find(SocketAddress(part.peer()));
            if (x == tosend.end()) {
                std::pair p{std::make_unique<Connection>(SocketAddress(part.peer())),
                            std::make_unique<cloud::CloudMessage>()};
                p.second->set_operation(cloud::CloudMessage_Operation_DROP_PARTITIONS);
                p.second->set_type(cloud::CloudMessage_Type_REQUEST);
                auto address = p.second->mutable_address();
                address->set_address(part.peer());
                auto tmp1 = p.second->add_partition();
                tmp1->set_id(part.id());
                tosend.insert({SocketAddress(part.peer()), std::move(p)});
            } else {
                auto tmp1 = x->second.second->add_partition();
                tmp1->set_id(part.id());
            }
        }
        Connection con1{routing.get_cluster_address().value()};
        con1.send(requestresponse);
        con1.receive(requestresponse);
        response.set_success(requestresponse.success());
        response.set_message(requestresponse.message());
        for (auto &s: tosend) {
            s.second.first->send(*s.second.second);
        }
        for (auto &r: tosend) {
            r.second.first->receive(*r.second.second);
            if (!r.second.second->success()) {
                response.set_success(false);
                response.set_message("ERROR");
            }
        }


        con.send(response);
    }

    auto P2PHandler::handle_drop_partitions(Connection &con,
                                            const cloud::CloudMessage &msg)
    -> void {
        cloud::CloudMessage response;
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_DROP_PARTITIONS);
        cloud::CloudMessage requestresponse;
        requestresponse.set_type(cloud::CloudMessage_Type_REQUEST);
        requestresponse.set_operation(cloud::CloudMessage_Operation_PARTITIONS_REMOVED);
        requestresponse.set_message("OK");
        requestresponse.set_success(true);
        for (auto &part: msg.partition()) {
            auto search = partitions.find(part.id());
            if (search != partitions.end()) partitions.erase(search);
            auto tmp = requestresponse.add_partition();
            tmp->set_peer(msg.address().address());
            tmp->set_id(part.id());
        }
        Connection con1{routing.get_cluster_address().value()};
        con1.send(requestresponse);
        con1.receive(requestresponse);

        response.set_message(requestresponse.message());
        response.set_success(requestresponse.success());
        con.send(response);

    }

    auto P2PHandler::handle_transfer_partition(Connection &con,
                                               const cloud::CloudMessage &msg)
    -> void {
        std::unordered_map<SocketAddress, std::pair<std::unique_ptr<Connection>, std::unique_ptr<cloud::CloudMessage>>> tosend;
        cloud::CloudMessage response;
        response.set_type(cloud::CloudMessage_Type_RESPONSE);
        response.set_operation(cloud::CloudMessage_Operation_TRANSFER_PARTITION);
        cloud::CloudMessage requestresponse;
        requestresponse.set_type(cloud::CloudMessage_Type_REQUEST);
        requestresponse.set_operation(cloud::CloudMessage_Operation_PARTITIONS_REMOVED);
        requestresponse.set_message("OK");
        requestresponse.set_success(true);

        for (auto &part: msg.partition()) {
            auto search = partitions.find(part.id());
            if (search != partitions.end()) partitions.erase(search);
            auto tmp = requestresponse.add_partition();
            tmp->set_peer(msg.address().address());
            tmp->set_id(part.id());
            auto x = tosend.find(SocketAddress(part.peer()));
            if (x == tosend.end()) {
                std::pair p{std::make_unique<Connection>(SocketAddress(part.peer())),
                            std::make_unique<cloud::CloudMessage>()};
                p.second->set_operation(cloud::CloudMessage_Operation_CREATE_PARTITIONS);
                p.second->set_type(cloud::CloudMessage_Type_REQUEST);
                auto address = p.second->mutable_address();
                address->set_address(part.peer());
                auto tmp1 = p.second->add_partition();
                tmp1->set_id(part.id());
                tosend.insert({SocketAddress(part.peer()), std::move(p)});
            } else {
                auto tmp1 = x->second.second->add_partition();
                tmp1->set_id(part.id());
            }
        }
        Connection con1{routing.get_cluster_address().value()};
        con1.send(requestresponse);
        con1.receive(requestresponse);
        response.set_success(requestresponse.success());
        response.set_message(requestresponse.message());
        for (auto &s: tosend) {
            s.second.first->send(*s.second.second);
        }

        for (auto &r: tosend) {
            r.second.first->receive(*r.second.second);
            if (!r.second.second->success()) {
                response.set_success(false);
                response.set_message("ERROR");
            }
        }
        con.send(response);
    }

}  // namespace cloudlab