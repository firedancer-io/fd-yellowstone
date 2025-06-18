# fd-yellowstone

The firedancer/fd-yellowstone repository
(https://github.com/firedancer-io/fd-yellowstone) is a sample geyser
implementation for the full firedancer client. It is meant to mimic
https://github.com/rpcpool/yellowstone-grpc.git .

To build:
1. Clone the repo under src/app in the firedancer source tree. For
example:
* cd firedancer/src/app
* git clone https://github.com/firedancer-io/fd-yellowstone
2. Install grpc (https://github.com/grpc/grpc) somewhere.
3. Tweak the PKG variable in Local.mk and Makefile to correspond to
your grpc installation.
4. Build firedancer normally. You should see a new executable
fd_grpc_geyser in the bin directory.
5. Run the firedancer full client. Wait until it is processing blocks.
6. In another window, run "fd_grpc_geyser". The --port flag can
specify the service port.
7. Try the unit test (unit-test/test_geyser_client) The --target flag
specifies host:port.
