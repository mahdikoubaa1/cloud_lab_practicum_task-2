#include "cloudlab/handler/p2p.hh"

#include "fmt/core.h"

#include "cloud.pb.h"

namespace cloudlab {

P2PHandler::P2PHandler(Routing& routing) : routing{routing} {
  auto hash = std::hash<SocketAddress>()(routing.get_backend_address());
  auto path = fmt::format("/tmp/{}-initial", hash);
  partitions.insert({0, std::make_unique<KVS>(path)});
}

auto P2PHandler::handle_connection(Connection& con) -> void {
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

auto P2PHandler::handle_put(Connection& con, const cloud::CloudMessage& msg)
    -> void {
  // TODO (you)
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  response.set_message("OK");

  for (const auto& kvp : msg.kvp()) {
    auto* tmp = response.add_kvp();
    tmp->set_key(kvp.key());
    if (kvs.put(kvp.key(), kvp.value())) {
      tmp->set_value("OK");
    } else {
      tmp->set_value("ERROR");
      response.set_success(false);
      response.set_message("ERROR");
    }
  }

  con.send(response);
}

auto P2PHandler::handle_get(Connection& con, const cloud::CloudMessage& msg)
    -> void {
  // TODO (you)
  cloud::CloudMessage response{};
  std::string value;

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  response.set_message("OK");

  for (const auto& kvp : msg.kvp()) {
    auto* tmp = response.add_kvp();
    tmp->set_key(kvp.key());
    if (kvs.get(kvp.key(), value)) {
      tmp->set_value(value);
    } else {
      tmp->set_value("ERROR");
    }
  }

  con.send(response);
}

auto P2PHandler::handle_delete(Connection& con, const cloud::CloudMessage& msg)
    -> void {
  // TODO (you)
  cloud::CloudMessage response{};
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  response.set_message("OK");

  for (const auto& kvp : msg.kvp()) {
    auto* tmp = response.add_kvp();
    tmp->set_key(kvp.key());

    if (kvs.remove(kvp.key())) {
      tmp->set_value("OK");
    } else {
      tmp->set_value("ERROR");
    }
  }

  con.send(response);
}

auto P2PHandler::handle_join_cluster(Connection& con,
                                     const cloud::CloudMessage& msg) -> void {
  // TODO (you)
}

auto P2PHandler::handle_create_partitions(Connection& con,
                                          const cloud::CloudMessage& msg)
    -> void {
  // TODO (you)
}

auto P2PHandler::handle_steal_partitions(Connection& con,
                                         const cloud::CloudMessage& msg)
    -> void {
  // TODO (you)
}

auto P2PHandler::handle_drop_partitions(Connection& con,
                                        const cloud::CloudMessage& msg)
    -> void {
  // TODO (you)
}

auto P2PHandler::handle_transfer_partition(Connection& con,
                                           const cloud::CloudMessage& msg)
    -> void {
  // TODO (you)
}

}  // namespace cloudlab