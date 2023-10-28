#include "cloudlab/handler/router.hh"

#include "fmt/core.h"

#include "cloud.pb.h"

namespace cloudlab {

auto RouterHandler::handle_connection(Connection& con) -> void {
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

auto RouterHandler::handle_key_operation(Connection& con,
                                         const cloud::CloudMessage& msg)
    -> void {
  // TODO (you)
}

auto RouterHandler::handle_join_cluster(Connection& con,
                                        const cloud::CloudMessage& msg)
    -> void {
  // TODO (you)
}

auto RouterHandler::add_new_node(const SocketAddress& peer) -> void {
  // TODO (you)
}

auto RouterHandler::redistribute_partitions() -> void {
  // TODO (you)
}

auto RouterHandler::handle_partitions_added(Connection& con,
                                            const cloud::CloudMessage& msg)
    -> void {
  // TODO (you)
}

auto RouterHandler::handle_partitions_removed(Connection& con,
                                              const cloud::CloudMessage& msg)
    -> void {
  // TODO (you)
}

}  // namespace cloudlab