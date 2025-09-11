#include "local-pragmas.h"

extern "C" {
#include "../firedancer/version.h"
#include "../../discof/replay/fd_replay_notif.h"
#include "../../ballet/base58/fd_base58.h"
#include "geys_filter.h"
}

#include <mutex>

#include "geys_methods.hxx"

GeyserServiceImpl::GeyserServiceImpl(geys_fd_ctx_t * loop_ctx)
  : filt_(geys_get_filter(loop_ctx))
{
  geys_filter_set_service(filt_, this);
}

GeyserServiceImpl::~GeyserServiceImpl() {
}

void
GeyserServiceImpl::notify(fd_replay_notif_msg_t * msg) {
  lastinfo_ = *msg;
  validhashes_[msg->slot_exec.block_hash] = msg->slot_exec.slot;
}

struct GeyserSubscribeReactor : public ::grpc::ServerBidiReactor<::geyser::SubscribeRequest, ::geyser::SubscribeUpdate> {
  GeyserSubscribeReactor(GeyserServiceImpl * serv, geys_filter_t * filter) : serv_(serv), filter_(filter) {
    std::scoped_lock smut(mut_);
    StartRead(&request_);
  }
  void OnReadDone(bool ok) override {
    if( request_.has_ping() ) {
      auto* update = new ::geyser::SubscribeUpdate();
      auto* pong = new ::geyser::SubscribeUpdatePong();
      update->set_allocated_pong(pong);
      pong->set_id(request_.ping().id());
      this->Update( update );
    }

    std::scoped_lock smut(mut_);
    if (ok) {
      geys_filter_add_sub(filter_, &request_, this);
      StartRead(&request_);
    } else {
      Finish(::grpc::Status::OK);
    }
  }

  void Update(::geyser::SubscribeUpdate* update) {
    std::scoped_lock smut(mut_);
    if( updates_.empty() ) {
      StartWrite( update );
    }
    updates_.push_back(update);
  }

  void OnWriteDone(bool ok) override {
    std::scoped_lock smut(mut_);
    if (ok) {
      auto i = updates_.begin();
      delete (::geyser::SubscribeUpdate*)(*i);
      updates_.erase(i);
      if( !updates_.empty() ) {
        StartWrite( *updates_.begin() );
      }
    }
  }

  void OnDone() override {
    {
      std::scoped_lock smut(mut_);
      geys_filter_un_sub(filter_, this);
    }
    delete this;
  }

  std::mutex mut_;
  GeyserServiceImpl * serv_ = NULL;
  geys_filter_t * filter_ = NULL;
  ::geyser::SubscribeRequest request_;
  std::vector<::geyser::SubscribeUpdate*> updates_;
};

::grpc::ServerBidiReactor<::geyser::SubscribeRequest, ::geyser::SubscribeUpdate>*
GeyserServiceImpl::Subscribe(::grpc::CallbackServerContext* context) {
  return new GeyserSubscribeReactor(this, filt_);
}

::grpc::ServerUnaryReactor*
GeyserServiceImpl::SubscribeReplayInfo(::grpc::CallbackServerContext* context, const ::geyser::SubscribeReplayInfoRequest* request, ::geyser::SubscribeReplayInfoResponse* response) {
  class Reactor : public grpc::ServerUnaryReactor {
    public:
      Reactor(GeyserServiceImpl * serv, const ::geyser::SubscribeReplayInfoRequest* request, ::geyser::SubscribeReplayInfoResponse* response) {
        Finish(grpc::Status::OK);
      }
      void OnDone() override {
        delete this;
      }
  };
  return new Reactor(this, request, response);
}

::grpc::ServerUnaryReactor*
GeyserServiceImpl::Ping(::grpc::CallbackServerContext* context, const ::geyser::PingRequest* request, ::geyser::PongResponse* response) {
  class Reactor : public grpc::ServerUnaryReactor {
    public:
      Reactor(GeyserServiceImpl * serv, const ::geyser::PingRequest* request, ::geyser::PongResponse* response) {
        response->set_count( request->count() );
        Finish(grpc::Status::OK);
      }
      void OnDone() override {
        delete this;
      }
  };
  return new Reactor(this, request, response);
}

::grpc::ServerUnaryReactor*
GeyserServiceImpl::GetLatestBlockhash(::grpc::CallbackServerContext* context, const ::geyser::GetLatestBlockhashRequest* request, ::geyser::GetLatestBlockhashResponse* response) {
  class Reactor : public grpc::ServerUnaryReactor {
    public:
      Reactor(GeyserServiceImpl * serv, const ::geyser::GetLatestBlockhashRequest* request, ::geyser::GetLatestBlockhashResponse* response) {
        response->set_slot( serv->lastinfo_.slot_exec.slot );
        FD_BASE58_ENCODE_32_BYTES( serv->lastinfo_.slot_exec.block_hash.uc, hash_str );
        response->set_blockhash( std::string(hash_str, hash_str_len) );
        response->set_last_valid_block_height( serv->lastinfo_.slot_exec.height );
        Finish(grpc::Status::OK);
      }
      void OnDone() override {
        delete this;
      }
  };
  return new Reactor(this, request, response);
}

::grpc::ServerUnaryReactor*
GeyserServiceImpl::GetBlockHeight(::grpc::CallbackServerContext* context, const ::geyser::GetBlockHeightRequest* request, ::geyser::GetBlockHeightResponse* response) {
  class Reactor : public grpc::ServerUnaryReactor {
    public:
      Reactor(GeyserServiceImpl * serv, const ::geyser::GetBlockHeightRequest* request, ::geyser::GetBlockHeightResponse* response) {
        response->set_block_height( serv->lastinfo_.slot_exec.height );
        Finish(grpc::Status::OK);
      }
      void OnDone() override {
        delete this;
      }
  };
  return new Reactor(this, request, response);
}

::grpc::ServerUnaryReactor*
GeyserServiceImpl::GetSlot(::grpc::CallbackServerContext* context, const ::geyser::GetSlotRequest* request, ::geyser::GetSlotResponse* response) {
  class Reactor : public grpc::ServerUnaryReactor {
    public:
      Reactor(GeyserServiceImpl * serv, const ::geyser::GetSlotRequest* request, ::geyser::GetSlotResponse* response) {
        response->set_slot( serv->lastinfo_.slot_exec.slot );
        Finish(grpc::Status::OK);
      }
      void OnDone() override {
        delete this;
      }
  };
  return new Reactor(this, request, response);
}

::grpc::ServerUnaryReactor*
GeyserServiceImpl::IsBlockhashValid(::grpc::CallbackServerContext* context, const ::geyser::IsBlockhashValidRequest* request, ::geyser::IsBlockhashValidResponse* response) {
  class Reactor : public grpc::ServerUnaryReactor {
    public:
      Reactor(GeyserServiceImpl * serv, const ::geyser::IsBlockhashValidRequest* request, ::geyser::IsBlockhashValidResponse* response) {
        fd_hash_t hash;
        if( fd_base58_decode_32( request->blockhash().c_str(), hash.uc ) == NULL ) {
          Finish(grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "failed to decode hash"));
          return;
        }
        auto i = serv->validhashes_.find(hash);
        if( i == serv->validhashes_.end() )
          response->set_valid(false);
        else {
          response->set_slot(i->second);
          response->set_valid(true);
        }
        Finish(grpc::Status::OK);
      }
      void OnDone() override {
        delete this;
      }
  };
  return new Reactor(this, request, response);
}

::grpc::ServerUnaryReactor*
GeyserServiceImpl::GetVersion(::grpc::CallbackServerContext* context, const ::geyser::GetVersionRequest* request, ::geyser::GetVersionResponse* response) {
  class Reactor : public grpc::ServerUnaryReactor {
    public:
      Reactor(GeyserServiceImpl * serv, const ::geyser::GetVersionRequest* request, ::geyser::GetVersionResponse* response) {
        response->set_version(FIREDANCER_VERSION);
        Finish(grpc::Status::OK);
      }
      void OnDone() override {
        delete this;
      }
  };
  return new Reactor(this, request, response);
}

static ::geyser::SubscribeUpdateAccountInfo *
getAcctInfo(ulong slot, fd_pubkey_t * key, fd_account_meta_t * meta, const uchar * val, ulong val_sz) {
  auto* info = new ::geyser::SubscribeUpdateAccountInfo();
  info->set_pubkey(key->uc, 32U);
  info->set_lamports(meta->info.lamports);
  info->set_owner(meta->info.owner, 32U);
  info->set_executable(meta->info.executable);
  info->set_data(val, val_sz);
  return info;
}

void
GeyserServiceImpl::updateAcct(GeyserSubscribeReactor_t * reactor, ulong slot, fd_pubkey_t * key, fd_account_meta_t * meta, const uchar * val, ulong val_sz) {
  auto* update = new ::geyser::SubscribeUpdate();
  auto* acct = new ::geyser::SubscribeUpdateAccount();
  update->set_allocated_account(acct);
  acct->set_slot(slot);
  acct->set_is_startup(false);
  auto* info = getAcctInfo( slot, key, meta, val, val_sz );
  acct->set_allocated_account(info);

  reactor->Update( update );
}

void
GeyserServiceImpl::updateSlot(GeyserSubscribeReactor_t * reactor, fd_replay_notif_msg_t * msg) {
  auto* update = new ::geyser::SubscribeUpdate();
  auto* slot = new ::geyser::SubscribeUpdateSlot();
  update->set_allocated_slot(slot);
  slot->set_slot(msg->slot_exec.slot);
  slot->set_parent(msg->slot_exec.parent);

  reactor->Update( update );
}

static ::geyser::SubscribeUpdateTransactionInfo *
getTxnInfo(fd_replay_notif_msg_t * msg, fd_txn_t * txn, fd_pubkey_t * accs, fd_ed25519_sig_t const * sigs) {
  auto* info = new ::geyser::SubscribeUpdateTransactionInfo();
  info->set_signature(sigs, 64);
  auto* txn3 = new ::solana::storage::ConfirmedBlock::Transaction();
  info->set_allocated_transaction(txn3);
  for( uint i = 0UL; i < txn->signature_cnt; i++ ) {
    txn3->mutable_signatures()->Add({(const char*)&sigs[i], 64});
  }
  auto* mess = new ::solana::storage::ConfirmedBlock::Message();
  txn3->set_allocated_message(mess);
  auto* head = new ::solana::storage::ConfirmedBlock::MessageHeader();
  mess->set_allocated_header(head);
  head->set_num_required_signatures( 1 );
  head->set_num_readonly_signed_accounts( txn->readonly_signed_cnt );
  head->set_num_readonly_unsigned_accounts( txn->readonly_unsigned_cnt );
  for( uint i = 0; i < txn->acct_addr_cnt; i++ ) {
    mess->mutable_account_keys()->Add({(const char*)&accs[i], 32});
  }
  mess->set_recent_blockhash(msg->slot_exec.block_hash.uc, 32);

  return info;
}

void
GeyserServiceImpl::updateTxn(GeyserSubscribeReactor_t * reactor, fd_replay_notif_msg_t * msg, fd_txn_t * txn, fd_pubkey_t * accs, fd_ed25519_sig_t const * sigs) {
  auto* update = new ::geyser::SubscribeUpdate();
  auto* txn2 = new ::geyser::SubscribeUpdateTransaction();
  update->set_allocated_transaction(txn2);
  txn2->set_slot(msg->slot_exec.slot);
  auto* info = getTxnInfo( msg, txn, accs, sigs );
  txn2->set_allocated_transaction(info);

  reactor->Update( update );
}

void
GeyserServiceImpl::addAcct(::geyser::SubscribeUpdateBlock * blk, ulong slot, fd_pubkey_t * key, fd_account_meta_t * meta, const uchar * val, ulong val_sz) {
  auto* info = getAcctInfo( slot, key, meta, val, val_sz );
  blk->mutable_accounts()->AddAllocated(info);
  blk->set_updated_account_count(blk->updated_account_count() + 1);
}

void
GeyserServiceImpl::addTxn(::geyser::SubscribeUpdateBlock * blk, fd_replay_notif_msg_t * msg, fd_txn_t * txn, fd_pubkey_t * accs, fd_ed25519_sig_t const * sigs) {
  auto* info = getTxnInfo( msg, txn, accs, sigs );
  blk->mutable_transactions()->AddAllocated(info);
  blk->set_executed_transaction_count(blk->executed_transaction_count() + 1);
}

void
GeyserServiceImpl::sendUpdateBlock( GeyserSubscribeReactor_t * reactor, ::geyser::SubscribeUpdateBlock * blk, fd_replay_notif_msg_t * msg ) {
  if( !blk ) return;
  blk->set_slot(msg->slot_exec.slot);
  FD_BASE58_ENCODE_32_BYTES( msg->slot_exec.block_hash.uc, hash_str );
  blk->set_blockhash( std::string(hash_str, hash_str_len) );
  auto * bh = new ::solana::storage::ConfirmedBlock::BlockHeight();
  bh->set_block_height(msg->slot_exec.height);
  blk->set_allocated_block_height(bh);
  blk->set_parent_slot(msg->slot_exec.parent);

  auto* update = new ::geyser::SubscribeUpdate();
  update->set_allocated_block(blk);
  reactor->Update( update );
}
