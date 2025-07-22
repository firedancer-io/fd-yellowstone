# fd-yellowstone

The [fd-yellowstone repository](https://github.com/firedancer-io/fd-yellowstone) is a sample geyser implementation for the full firedancer client.
It is meant to mimic the [original yellowstone-grpc](https://github.com/rpcpool/yellowstone-grpc).

## Build

1. Clone the repo under `src/app` in the **firedancer** source tree.
For example:

```console
$ cd firedancer/src/app
$ git clone https://github.com/firedancer-io/fd-yellowstone
```

2. Install [grpc](https://github.com/grpc/grpc) somewhere.
3. Tweak the `PKG` and `PROTOS_PATH` variables in `Local.mk` and `Makefile` to correspond to your grpc installation.
4. Build **firedancer** normally. You should see a new executable
`fd_grpc_geyser` in the `bin` directory.

## Run

1. Run the **firedancer** full client. _Wait until it is processing blocks._
2. In another window, run `fd_grpc_geyser`.
Available flags are:
   
| Flag                | Default                 | Description                 |
| ------------------- | ----------------------- | --------------------------- |
| `--port`            | 8754                    | Server port for the service |
| `--funk_wksp`       | "fd1_funk.wksp"         | Funk workspace              |
| `--blockstore_wksp` | "fd1_blockstore.wksp"   | Blockstore workspace        |
| `--notify_wksp`     | "fd1_replay_notif.wksp" | Notification link workspace |

## Testing

Try the unit test `unit-test/test_geyser_client`

| Flag       | Default          | Description    |
| ---------- | ---------------- | -------------- |
| `--target` | "localhost:8754" | Server address |