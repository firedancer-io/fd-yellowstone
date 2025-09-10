#include "local-pragmas.h"

#include "geys_methods.hxx"

#include <iostream>
#include <memory>
#include <string>
#include <stdio.h>
#include <thread>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/initialize.h"
#include "absl/strings/str_format.h"

extern "C" {
#include "geys_fd_loop.h"
}

ABSL_FLAG(uint16_t, port, 8754, "Server port for the service");
ABSL_FLAG(std::string, funk_wksp, "fd1_funk.wksp", "Funk workspace");
ABSL_FLAG(std::string, store_wksp, "fd1_store.wksp", "Store workspace");
ABSL_FLAG(std::string, notify_wksp, "fd1_replay_notif.wksp", "Source of replay notifications");
ABSL_FLAG(std::string, repair_wksp, "fd1_repair_repla.wksp", "Source of fec sets");

void
RunServer(uint16_t port, geys_fd_ctx_t * loop_ctx) {
  std::string server_address = absl::StrFormat("0.0.0.0:%d", port);
  GeyserServiceImpl service( loop_ctx );

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  grpc::ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // One thread for development
  grpc::ResourceQuota quota;
  quota.SetMaxThreads(1);
  builder.SetResourceQuota(quota);
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  fd_boot( &argc, &argv );

  absl::ParseCommandLine(argc, argv);
  absl::InitializeLog();

  geys_fd_loop_args_t loop_args;
  strncpy(loop_args.funk_wksp, absl::GetFlag(FLAGS_funk_wksp).c_str(), 32-1);
  strncpy(loop_args.store_wksp, absl::GetFlag(FLAGS_store_wksp).c_str(), 32-1);
  strncpy(loop_args.notify_wksp, absl::GetFlag(FLAGS_notify_wksp).c_str(), 32-1);
  strncpy(loop_args.repair_wksp, absl::GetFlag(FLAGS_repair_wksp).c_str(), 32-1);
  geys_fd_ctx_t * loop_ctx = geys_fd_init( &loop_args );

  std::thread tid([loop_ctx](){ geys_fd_loop(loop_ctx); });

  RunServer(absl::GetFlag(FLAGS_port), loop_ctx);

  tid.join();

  fd_halt();
  return 0;
}
