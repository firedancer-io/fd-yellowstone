#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "geyser.grpc.pb.h"

#include <map>

extern "C" {
#include "geys_fd_loop.h"
#include "geys_filter.h"
#include "../../discof/replay/fd_replay_notif.h"
}

struct fd_hash_cmp {
    bool operator() (const fd_hash_t& a, const fd_hash_t& b) const {
      for( uint i = 0; i < 4; ++i )
        if( a.ul[i] != b.ul[i] )
          return ( a.ul[i] < b.ul[i] );
      return false;
    }
};

class GeyserServiceImpl final : public geyser::Geyser::CallbackService{
    geys_filter_t * filt_;
    fd_replay_notif_msg_t lastinfo_;
    std::map<fd_hash_t,ulong,fd_hash_cmp> validhashes_;

  public:
    GeyserServiceImpl(geys_fd_ctx_t * loop_ctx);

    virtual ~GeyserServiceImpl() override;

    virtual ::grpc::ServerBidiReactor< ::geyser::SubscribeRequest, ::geyser::SubscribeUpdate>* Subscribe(
      ::grpc::CallbackServerContext* /*context*/) override;

    virtual ::grpc::ServerUnaryReactor* SubscribeReplayInfo(
      ::grpc::CallbackServerContext* /*context*/, const ::geyser::SubscribeReplayInfoRequest* /*request*/, ::geyser::SubscribeReplayInfoResponse* /*response*/) override;

    virtual ::grpc::ServerUnaryReactor* Ping(
      ::grpc::CallbackServerContext* /*context*/, const ::geyser::PingRequest* /*request*/, ::geyser::PongResponse* /*response*/) override;

    virtual ::grpc::ServerUnaryReactor* GetLatestBlockhash(
      ::grpc::CallbackServerContext* /*context*/, const ::geyser::GetLatestBlockhashRequest* /*request*/, ::geyser::GetLatestBlockhashResponse* /*response*/) override;

    virtual ::grpc::ServerUnaryReactor* GetBlockHeight(
      ::grpc::CallbackServerContext* /*context*/, const ::geyser::GetBlockHeightRequest* /*request*/, ::geyser::GetBlockHeightResponse* /*response*/) override;

    virtual ::grpc::ServerUnaryReactor* GetSlot(
      ::grpc::CallbackServerContext* /*context*/, const ::geyser::GetSlotRequest* /*request*/, ::geyser::GetSlotResponse* /*response*/) override;

    virtual ::grpc::ServerUnaryReactor* IsBlockhashValid(
      ::grpc::CallbackServerContext* /*context*/, const ::geyser::IsBlockhashValidRequest* /*request*/, ::geyser::IsBlockhashValidResponse* /*response*/) override;

    virtual ::grpc::ServerUnaryReactor* GetVersion(
      ::grpc::CallbackServerContext* /*context*/, const ::geyser::GetVersionRequest* /*request*/, ::geyser::GetVersionResponse* /*response*/) override;

    void notify(fd_replay_notif_msg_t * msg);

    static void updateAcct(GeyserSubscribeReactor_t * reactor, ulong slot, fd_pubkey_t * key, fd_ed25519_sig_t const * sig, fd_account_meta_t * meta, const uchar * val, ulong val_sz) ;
    static void updateSlot(GeyserSubscribeReactor_t * reactor, fd_replay_notif_msg_t * msg);
    static void updateTxn(GeyserSubscribeReactor_t * reactor, fd_replay_notif_msg_t * msg, fd_txn_t * txn, fd_pubkey_t * accs, fd_ed25519_sig_t const * sigs);

    static void addAcct(::geyser::SubscribeUpdateBlock * blk, ulong slot, fd_pubkey_t * key, fd_ed25519_sig_t const * sig, fd_account_meta_t * meta, const uchar * val, ulong val_sz) ;
    static void addTxn(::geyser::SubscribeUpdateBlock * blk, fd_replay_notif_msg_t * msg, fd_txn_t * txn, fd_pubkey_t * accs, fd_ed25519_sig_t const * sigs);
    static void sendUpdateBlock( GeyserSubscribeReactor_t * reactor, ::geyser::SubscribeUpdateBlock * blk, fd_replay_notif_msg_t * msg );
};
