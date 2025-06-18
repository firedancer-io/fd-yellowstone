#include <grpcpp/grpcpp.h>

#include <iostream>
#include <memory>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "geyser.grpc.pb.h"

extern "C" {
#include "../../ballet/base58/fd_base58.h"
}

ABSL_FLAG(std::string, target, "localhost:8754", "Server address");

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

void
printUpdate(::geyser::SubscribeUpdate const * update) {
  if( update->update_oneof_case() == ::geyser::SubscribeUpdate::kAccount ) {
    auto& acct = update->account();
    auto& info = acct.account();
    FD_BASE58_ENCODE_32_BYTES((uchar const *)info.pubkey().c_str(), pubkey);
    FD_BASE58_ENCODE_32_BYTES((uchar const *)info.owner().c_str(), owner);
    std::cout <<
      "account\n" <<
      "  slot=" << acct.slot() << "\n" <<
      "  is_startup=" << acct.is_startup() << "\n" <<
      "  pubkey=" << std::string(pubkey, pubkey_len) << "\n" <<
      "  lamports=" << info.lamports() << "\n" <<
      "  owner=" << std::string(owner, owner_len) << "\n" <<
      "  executable=" << info.executable() << "\n";
  }

  if( update->update_oneof_case() == ::geyser::SubscribeUpdate::kSlot ) {
    auto& slot = update->slot();
    std::cout <<
      "slot\n" <<
      "  slot=" << slot.slot() << "\n" <<
      "  parent=" << slot.parent() << "\n";
  }

  if( update->update_oneof_case() == ::geyser::SubscribeUpdate::kTransaction ) {
    auto& txn2 = update->transaction();
    auto& info = txn2.transaction();
    auto& txn3 = info.transaction();
    auto& mess = txn3.message();
    auto& head = mess.header();
    std::cout <<
      "transaction\n" <<
      "  slot=" << txn2.slot() << "\n" <<
      "  num_required_signatures=" << head.num_required_signatures() << "\n" <<
      "  num_readonly_signed_accounts=" << head.num_readonly_signed_accounts() << "\n" <<
      "  num_readonly_unsigned_accounts=" << head.num_readonly_unsigned_accounts() << "\n" <<
      "  signatures\n";
    for( int i = 0; i < txn3.signatures_size(); i++ ) {
      FD_BASE58_ENCODE_64_BYTES((const uchar*)txn3.signatures(i).c_str(), sig);
      std::cout << "    " << std::string(sig, sig_len) << "\n";
    }
    std::cout << "  accounts\n";
    for( int i = 0; i < mess.account_keys_size(); i++ ) {
      FD_BASE58_ENCODE_32_BYTES((const uchar*)mess.account_keys(i).c_str(), acct);
      std::cout << "    " << std::string(acct, acct_len) << "\n";
    }
    {
      FD_BASE58_ENCODE_32_BYTES((const uchar*)mess.recent_blockhash().c_str(), hash);
      std::cout << "  block_hash=" << std::string(hash, hash_len) << "\n";
    }
  }

  if( update->update_oneof_case() == ::geyser::SubscribeUpdate::kBlock ) {
    auto& blk = update->block();
    std::cout <<
      "block\n" <<
      "  slot=" << blk.slot() << "\n" <<
      "  block_hash=" << blk.blockhash() << "\n" <<
      "  height=" << blk.block_height().block_height() << "\n" <<
      "  parent_slot=" << blk.parent_slot() << "\n";
    for( auto& info : blk.transactions() ) {
      auto& txn = info.transaction();
      auto& mess = txn.message();
      auto& head = mess.header();
      std::cout <<
        "  transaction\n" <<
        "    num_required_signatures=" << head.num_required_signatures() << "\n" <<
        "    num_readonly_signed_accounts=" << head.num_readonly_signed_accounts() << "\n" <<
        "    num_readonly_unsigned_accounts=" << head.num_readonly_unsigned_accounts() << "\n" <<
        "    signatures\n";
      for( int i = 0; i < txn.signatures_size(); i++ ) {
        FD_BASE58_ENCODE_64_BYTES((const uchar*)txn.signatures(i).c_str(), sig);
        std::cout << "      " << std::string(sig, sig_len) << "\n";
      }
      std::cout << "    accounts\n";
      for( int i = 0; i < mess.account_keys_size(); i++ ) {
        FD_BASE58_ENCODE_32_BYTES((const uchar*)mess.account_keys(i).c_str(), acct);
        std::cout << "      " << std::string(acct, acct_len) << "\n";
      }
    }
    for( auto& info : blk.accounts() ) {
      FD_BASE58_ENCODE_32_BYTES((uchar const *)info.pubkey().c_str(), pubkey);
      FD_BASE58_ENCODE_32_BYTES((uchar const *)info.owner().c_str(), owner);
      std::cout <<
        "  account\n" <<
        "    pubkey=" << std::string(pubkey, pubkey_len) << "\n" <<
        "    lamports=" << info.lamports() << "\n" <<
        "    owner=" << std::string(owner, owner_len) << "\n" <<
        "    executable=" << info.executable() << "\n";
    }
  }
}

class GeyserClient {
  public:
    GeyserClient(std::shared_ptr<Channel> channel)
      : stub_(geyser::Geyser::NewStub(channel)) {}

    void testGetVersion() {
      // Data we are sending to the server.
      ::geyser::GetVersionRequest request;

      // Container for the data we expect from the server.
      ::geyser::GetVersionResponse reply;

      // Context for the client. It could be used to convey extra information to
      // the server and/or tweak certain RPC behaviors.
      ClientContext context;

      // The actual RPC.
      Status status = stub_->GetVersion(&context, request, &reply);

      // Act upon its status.
      if (status.ok()) {
        std::cout << "version=" << reply.version() << std::endl;
      } else {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
      }
    }

    void testPing() {
      // Data we are sending to the server.
      ::geyser::PingRequest request;
      request.set_count(1234);

      // Container for the data we expect from the server.
      ::geyser::PongResponse reply;

      // Context for the client. It could be used to convey extra information to
      // the server and/or tweak certain RPC behaviors.
      ClientContext context;

      // The actual RPC.
      Status status = stub_->Ping(&context, request, &reply);

      // Act upon its status.
      if (status.ok()) {
        std::cout << "pong=" << reply.count() << std::endl;
      } else {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
      }
    }

    void testGetSlot() {
      // Data we are sending to the server.
      ::geyser::GetSlotRequest request;

      // Container for the data we expect from the server.
      ::geyser::GetSlotResponse reply;

      // Context for the client. It could be used to convey extra information to
      // the server and/or tweak certain RPC behaviors.
      ClientContext context;

      // The actual RPC.
      Status status = stub_->GetSlot(&context, request, &reply);

      // Act upon its status.
      if (status.ok()) {
        std::cout << "slot=" << reply.slot() << std::endl;
      } else {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
      }
    }

    void testGetBlockHeight() {
      // Data we are sending to the server.
      ::geyser::GetBlockHeightRequest request;

      // Container for the data we expect from the server.
      ::geyser::GetBlockHeightResponse reply;

      // Context for the client. It could be used to convey extra information to
      // the server and/or tweak certain RPC behaviors.
      ClientContext context;

      // The actual RPC.
      Status status = stub_->GetBlockHeight(&context, request, &reply);

      // Act upon its status.
      if (status.ok()) {
        std::cout << "block_height=" << reply.block_height() << std::endl;
      } else {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
      }
    }

    void testGetLatestBlockhash() {
      // Data we are sending to the server.
      ::geyser::GetLatestBlockhashRequest request;

      // Container for the data we expect from the server.
      ::geyser::GetLatestBlockhashResponse reply;

      {
        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        ClientContext context;

        // The actual RPC.
        Status status = stub_->GetLatestBlockhash(&context, request, &reply);

        // Act upon its status.
        if (status.ok()) {
          std::cout << "slot=" << reply.slot() << std::endl
                    << "hash=" << reply.blockhash() << std::endl
                    << "height=" << reply.last_valid_block_height() << std::endl;
        } else {
          std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        }
      }

      {
        // Data we are sending to the server.
        ::geyser::IsBlockhashValidRequest request2;
        request2.set_blockhash( reply.blockhash() );

        // Container for the data we expect from the server.
        ::geyser::IsBlockhashValidResponse reply2;

        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        ClientContext context;

        // The actual RPC.
        Status status = stub_->IsBlockhashValid(&context, request2, &reply2);

        // Act upon its status.
        if (status.ok()) {
          std::cout << "valid=" << reply2.valid() << std::endl;
        } else {
          std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        }
      }
    }

    void testSubscribePing() {
      // Data we are sending to the server.
      ::geyser::SubscribeRequest request;

      auto* a = new ::geyser::SubscribeRequestPing();
      a->set_id(1234);
      request.set_allocated_ping(a);

      // Context for the client. It could be used to convey extra information to
      // the server and/or tweak certain RPC behaviors.
      ClientContext context;

      // The actual RPC.
      auto rpc(stub_->Subscribe(&context));
      std::cout << "ping\n";
      rpc->Write(request);

      for(;;) {
        ::geyser::SubscribeUpdate update;
        if( rpc->Read(&update) ) {
          if( update.update_oneof_case() == ::geyser::SubscribeUpdate::kPong ) {
            assert(update.pong().id() == 1234);
            std::cout << "pong\n";
            break;

          }
        }
      }

      rpc->WritesDone();
    }

    void testSubscribe() {
      // Data we are sending to the server.
      ::geyser::SubscribeRequest request;

      ::geyser::SubscribeRequestFilterAccounts a;
      a.add_owner()->operator=("Vote111111111111111111111111111111111111111");
      request.mutable_accounts()->insert({"test", a});

      ::geyser::SubscribeRequestFilterSlots b;
      request.mutable_slots()->insert({"test2", b});

      ::geyser::SubscribeRequestFilterTransactions c;
      c.add_account_required()->operator=("Vote111111111111111111111111111111111111111");
      request.mutable_transactions()->insert({"test3", c});

      // Context for the client. It could be used to convey extra information to
      // the server and/or tweak certain RPC behaviors.
      ClientContext context;

      // The actual RPC.
      auto rpc(stub_->Subscribe(&context));
      rpc->Write(request);

      for( unsigned cnt = 0; cnt < 50; ) {
        ::geyser::SubscribeUpdate update;
        if( rpc->Read(&update) ) {
          printUpdate(&update);
          ++cnt;
        }
      }

      rpc->WritesDone();
    }

    void testSubscribeBlocks() {
      // Data we are sending to the server.
      ::geyser::SubscribeRequest request;

      ::geyser::SubscribeRequestFilterBlocks a;
      a.set_include_transactions(true);
      a.set_include_accounts(true);
      request.mutable_blocks()->insert({"test", a});

      // Context for the client. It could be used to convey extra information to
      // the server and/or tweak certain RPC behaviors.
      ClientContext context;

      // The actual RPC.
      auto rpc(stub_->Subscribe(&context));
      rpc->Write(request);

      for( unsigned cnt = 0; cnt < 10; ) {
        ::geyser::SubscribeUpdate update;
        if( rpc->Read(&update) ) {
          printUpdate(&update);
          ++cnt;
        }
      }

      rpc->WritesDone();
    }

  private:
    std::unique_ptr<geyser::Geyser::Stub> stub_;
};

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint specified by
  // the argument "--target=" which is the only expected argument.
  std::string target_str = absl::GetFlag(FLAGS_target);
  // We indicate that the channel isn't authenticated (use of
  // InsecureChannelCredentials()).
  GeyserClient geyser(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));

  geyser.testGetVersion();
  geyser.testPing();
  geyser.testGetSlot();
  geyser.testGetBlockHeight();
  geyser.testGetLatestBlockhash();
  geyser.testSubscribePing();
  geyser.testSubscribe();
  geyser.testSubscribeBlocks();

  return 0;
}
