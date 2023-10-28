#include "cloudlab/handler/router.hh"
#include "cloudlab/handler/api.hh"
#include "cloudlab/network/server.hh"

#include "argh.hh"
#include <fmt/core.h>

using namespace cloudlab;

auto main(int argc, char* argv[]) -> int {
  argh::parser cmdl({"-a", "--api", "-r", "--router"});
  cmdl.parse(argc, argv);

  std::string api_address, router_address;
  cmdl({"-a", "--api"}, "127.0.0.1:31000") >> api_address;
  cmdl({"-r", "--router"}, "127.0.0.1:33000") >> router_address;

  auto routing = Routing(router_address);

  auto api_handler = APIHandler(routing);
  auto api_server = Server(api_address, api_handler);
  auto api_thread = api_server.run();

  auto router_handler = RouterHandler(routing);
  auto router_server = Server(router_address, router_handler);
  auto router_thread = router_server.run();

  fmt::print("Router up and running ...\n");

  api_thread.join();
  router_thread.join();
}