#include "local-pragmas.h"

#include <mutex>
#include <vector>

#include "geyser.grpc.pb.h"
#include "geys_methods.hxx"

extern "C" {
#include "geys_filter.h"
#include "../../ballet/txn/fd_txn.h"
#include "../../discof/replay/fd_replay_notif.h"
#include "../../flamenco/runtime/fd_acc_mgr.h"
#include "../../ballet/block/fd_microblock.h"
#include "../../ballet/base58/fd_base58.h"
}

struct CompiledFilterAccount {
    std::string name_;
    std::vector<fd_hash_t> keys_;
    std::vector<fd_hash_t> owners_;
};

struct CompiledFilterSlot {
    std::string name_;
};

struct CompiledFilterTxn {
    std::string name_;
    std::vector<fd_hash_t> acct_include_;
    std::vector<fd_hash_t> acct_exclude_;
    std::vector<fd_hash_t> acct_required_;
};

struct CompiledFilterBlock {
    std::string name_;
    std::vector<fd_hash_t> acct_include_;
    bool include_transactions_;
    bool include_accounts_;
    ::geyser::SubscribeUpdateBlock * currentBlock_ = NULL;
};

class CompiledFilter {
  public:
    static CompiledFilter * compile(::geyser::SubscribeRequest * request) {
      CompiledFilter * filt = new CompiledFilter();
      if( filt->compile_internal(request) )
        return filt;
      delete filt;
      return NULL;
    }

    bool filterAccount(fd_pubkey_t * key, fd_account_meta_t * meta, const uchar * val, ulong val_sz);
    bool filterSlot(fd_replay_notif_msg_t * msg);
    bool filterTxn(fd_replay_notif_msg_t * msg, fd_txn_t * txn, fd_pubkey_t * accs, fd_ed25519_sig_t const * sigs);

    bool compile_internal(::geyser::SubscribeRequest * request);

    std::vector<std::unique_ptr<CompiledFilterAccount>> accts_;
    std::vector<std::unique_ptr<CompiledFilterSlot>> slots_;
    std::vector<std::unique_ptr<CompiledFilterTxn>> txns_;
    std::vector<std::unique_ptr<CompiledFilterBlock>> blks_;
};

bool
CompiledFilter::compile_internal(::geyser::SubscribeRequest * request) {
  bool hasfilter = false;

  for( auto& i : request->accounts() ) {
    auto* a = new CompiledFilterAccount();
    a->name_ = i.first;
    auto& f = i.second;
    for( int j = 0; j < f.account_size(); ++j ) {
      auto& s = f.account(j);
      fd_hash_t hash;
      if( !fd_base58_decode_32( s.c_str(), hash.uc ) ) return false;
      a->keys_.push_back(hash);
    }
    for( int j = 0; j < f.owner_size(); ++j ) {
      auto& s = f.owner(j);
      fd_hash_t hash;
      if( !fd_base58_decode_32( s.c_str(), hash.uc ) ) return false;
      a->owners_.push_back(hash);
    }
    accts_.emplace_back(a);
    hasfilter = true;
  }

  for( auto& i : request->slots() ) {
    auto* a = new CompiledFilterSlot();
    a->name_ = i.first;
    slots_.emplace_back(a);
    hasfilter = true;
  }

  for( auto& i : request->transactions() ) {
    auto* a = new CompiledFilterTxn();
    a->name_ = i.first;
    auto& f = i.second;
    for( int j = 0; j < f.account_include_size(); ++j ) {
      auto& s = f.account_include(j);
      fd_hash_t hash;
      if( !fd_base58_decode_32( s.c_str(), hash.uc ) ) return false;
      a->acct_include_.push_back(hash);
    }
    for( int j = 0; j < f.account_exclude_size(); ++j ) {
      auto& s = f.account_exclude(j);
      fd_hash_t hash;
      if( !fd_base58_decode_32( s.c_str(), hash.uc ) ) return false;
      a->acct_exclude_.push_back(hash);
    }
    for( int j = 0; j < f.account_required_size(); ++j ) {
      auto& s = f.account_required(j);
      fd_hash_t hash;
      if( !fd_base58_decode_32( s.c_str(), hash.uc ) ) return false;
      a->acct_required_.push_back(hash);
    }
    txns_.emplace_back(a);
    hasfilter = true;
  }

  for( auto& i : request->blocks() ) {
    auto* a = new CompiledFilterBlock();
    a->name_ = i.first;
    auto& f = i.second;
    for( int j = 0; j < f.account_include_size(); ++j ) {
      auto& s = f.account_include(j);
      fd_hash_t hash;
      if( !fd_base58_decode_32( s.c_str(), hash.uc ) ) return false;
      a->acct_include_.push_back(hash);
    }
    a->include_transactions_ = f.include_transactions();
    a->include_accounts_ = f.include_accounts();
    blks_.emplace_back(a);
    hasfilter = true;
  }

  return hasfilter;
}

bool
CompiledFilter::filterAccount(fd_pubkey_t * key, fd_account_meta_t * meta, const uchar * val, ulong val_sz) {
  for( auto& f : accts_ ) {
    for( auto& h : f->keys_ ) {
      if( !memcmp( h.uc, key->uc, 32 ) ) return true;
    }
    for( auto& h : f->owners_ ) {
      if( !memcmp( h.uc, meta->info.owner, 32 ) ) return true;
    }
  }
  return false;
}

bool
CompiledFilter::filterSlot(fd_replay_notif_msg_t * msg) {
  if( !slots_.empty() ) {
    return true;
  }
  return false;
}

bool
CompiledFilter::filterTxn(fd_replay_notif_msg_t * msg, fd_txn_t * txn, fd_pubkey_t * accs, fd_ed25519_sig_t const * sigs) {
  for( auto& f : txns_ ) {
    if( !f->acct_include_.empty() ) {
      for( auto& h : f->acct_include_ ) {
        for( uint j = 0; j < txn->acct_addr_cnt; ++j ) {
          if( !memcmp( h.uc, accs[j].uc, 32 ) ) goto success;
        }
      }
      return false;
      success: ;
    }
    if( !f->acct_exclude_.empty() ) {
      for( auto& h : f->acct_exclude_ ) {
        for( uint j = 0; j < txn->acct_addr_cnt; ++j ) {
          if( !memcmp( h.uc, accs[j].uc, 32 ) ) return false;
        }
      }
    }
    if( !f->acct_required_.empty() ) {
      for( auto& h : f->acct_required_ ) {
        for( uint j = 0; j < txn->acct_addr_cnt; ++j ) {
          if( !memcmp( h.uc, accs[j].uc, 32 ) ) goto success2;
        }
        return false;
        success2: ;
      }
    }
    return true;
  }
  return false;
}

struct geys_filter {
    struct Elem {
        CompiledFilter * filter_;
        GeyserSubscribeReactor * reactor_;
    };
    std::vector<Elem> elems_;
    fd_spad_t * spad_;
    fd_funk_t * funk_;
    GeyserServiceImpl * serv_;
    bool load_accts_ = false;

    geys_filter(fd_spad_t * spad, fd_funk_t * funk) : spad_(spad), funk_(funk) { }
    void notify_acct(ulong slot, fd_pubkey_t * key, fd_account_meta_t * meta, const uchar * val, ulong val_sz);
    void notify_slot(fd_replay_notif_msg_t * msg);
    void notify_txn(fd_replay_notif_msg_t * msg, fd_txn_t * txn, fd_pubkey_t * accs, fd_ed25519_sig_t const * sigs);
};

geys_filter_t *
geys_filter_create(fd_spad_t * spad, fd_funk_t * funk) {
  return new geys_filter(spad, funk);
}

void
geys_filter_set_service(geys_filter_t * filter, /* GeyserServiceImpl */ void * serv) {
  filter->serv_ = (GeyserServiceImpl *)serv;
}

void
geys_filter_add_sub(geys_filter_t * filter, /* SubscribeRequest*/ void * request_void, GeyserSubscribeReactor_t * reactor) {
  auto* request = (::geyser::SubscribeRequest *) request_void;
  auto* f = CompiledFilter::compile(request);
  if( !f) return;
  filter->elems_.push_back( { f, reactor } );
  if( !request->accounts().empty() || !request->blocks().empty())
    filter->load_accts_ = true;
}

void
geys_filter_un_sub(geys_filter_t * filter, GeyserSubscribeReactor_t * reactor) {
  for( auto i = filter->elems_.begin(); i != filter->elems_.end(); ) {
    if( i->reactor_ == reactor ) {
      delete i->filter_;
      i = filter->elems_.erase(i);
    } else
      ++i;
  }
}

void
geys_filter::notify_acct(ulong slot, fd_pubkey_t * key, fd_account_meta_t * meta, const uchar * val, ulong val_sz) {
  for( auto& i : elems_ ) {
    if( i.filter_->filterAccount(key, meta, val, val_sz) ) {
      GeyserServiceImpl::updateAcct(i.reactor_, slot, key, meta, val, val_sz);
    }
    for( auto& j : i.filter_->blks_ ) {
      if( j->include_accounts_ ) {
        if( !j->currentBlock_ ) j->currentBlock_ = new ::geyser::SubscribeUpdateBlock();
        GeyserServiceImpl::addAcct(j->currentBlock_, slot, key, meta, val, val_sz);
      }
    }
  }
}

void
geys_filter::notify_slot(fd_replay_notif_msg_t * msg) {
  for( auto& i : elems_ ) {
    if( i.filter_->filterSlot(msg) ) {
      GeyserServiceImpl::updateSlot(i.reactor_, msg);
    }
  }
}

void
geys_filter::notify_txn(fd_replay_notif_msg_t * msg, fd_txn_t * txn, fd_pubkey_t * accs, fd_ed25519_sig_t const * sigs) {
  for( auto& i : elems_ ) {
    if( i.filter_->filterTxn(msg, txn, accs, sigs) ) {
      GeyserServiceImpl::updateTxn(i.reactor_, msg, txn, accs, sigs);
    }
    for( auto& j : i.filter_->blks_ ) {
      if( j->include_transactions_ ) {
        if( !j->currentBlock_ ) j->currentBlock_ = new ::geyser::SubscribeUpdateBlock();
        GeyserServiceImpl::addTxn(j->currentBlock_, msg, txn, accs, sigs);
      }
    }
  }
}

void
geys_filter_notify(geys_filter_t * filter, fd_replay_notif_msg_t * msg, uchar * blk_data, ulong blk_sz) {
  filter->serv_->notify(msg);

  filter->notify_slot(msg);

  fd_funk_txn_map_t * txn_map = fd_funk_txn_map( filter->funk_ );
  fd_funk_txn_xid_t xid;
  xid.ul[0] = xid.ul[1] = msg->slot_exec.slot;
  fd_funk_txn_t * funk_txn = fd_funk_txn_query( &xid, txn_map );

  ulong blockoff = 0;
  while (blockoff < blk_sz) {
    if ( blockoff + sizeof(ulong) > blk_sz )
      return;
    ulong mcount = *(const ulong *)(blk_data + blockoff);
    blockoff += sizeof(ulong);

    /* Loop across microblocks */
    for (ulong mblk = 0; mblk < mcount; ++mblk) {
      if ( blockoff + sizeof(fd_microblock_hdr_t) > blk_sz )
        FD_LOG_ERR(("premature end of block"));
      fd_microblock_hdr_t * hdr = (fd_microblock_hdr_t *)((const uchar *)blk_data + blockoff);
      blockoff += sizeof(fd_microblock_hdr_t);

      /* Loop across transactions */
      for ( ulong txn_idx = 0; txn_idx < hdr->txn_cnt; txn_idx++ ) {
        uchar txn_out[FD_TXN_MAX_SZ];
        ulong pay_sz = 0;
        const uchar* raw = (const uchar *)blk_data + blockoff;
        ulong txn_sz = fd_txn_parse_core(raw, fd_ulong_min(blk_sz - blockoff, FD_TXN_MTU), txn_out, NULL, &pay_sz);
        if ( txn_sz == 0 || txn_sz > FD_TXN_MAX_SZ ) {
          FD_LOG_ERR( ( "failed to parse transaction %lu in microblock %lu", txn_idx, mblk ) );
        }
        fd_txn_t * txn = (fd_txn_t *)txn_out;

        fd_pubkey_t * accs = (fd_pubkey_t *)((uchar *)raw + txn->acct_addr_off);
        fd_ed25519_sig_t const * sigs = (fd_ed25519_sig_t const *)(raw + txn->signature_off);
        filter->notify_txn( msg, txn, accs, sigs );

        /* Loop across accoounts */
        if( filter->load_accts_ ) {
          for( int i = 0; i < txn->acct_addr_cnt; i++ ) {
            bool writable = ((i < txn->signature_cnt - txn->readonly_signed_cnt) ||
                             ((i >= txn->signature_cnt) && (i < txn->acct_addr_cnt - txn->readonly_unsigned_cnt)));
            if( !writable ) continue;

            fd_spad_push(filter->spad_);

            fd_funk_rec_key_t recid = fd_funk_acc_key(&accs[i]);
            ulong val_sz;
            const uchar * val = (const uchar *) fd_funk_rec_query_copy( filter->funk_, funk_txn, &recid, fd_spad_virtual(filter->spad_), &val_sz );
            if( val ) {
              filter->notify_acct(msg->slot_exec.slot, &accs[i], (fd_account_meta_t *)val, val + sizeof(fd_account_meta_t), val_sz - sizeof(fd_account_meta_t));
            }

            fd_spad_pop(filter->spad_);
          }
        }

        blockoff += pay_sz;
      }
    }
  }
  if ( blockoff != blk_sz )
    FD_LOG_ERR(("garbage at end of block"));

  for( auto& i : filter->elems_ ) {
    for( auto& j : i.filter_->blks_ ) {
      GeyserServiceImpl::sendUpdateBlock( i.reactor_, j->currentBlock_, msg );
      j->currentBlock_ = NULL;
    }
  }
}
