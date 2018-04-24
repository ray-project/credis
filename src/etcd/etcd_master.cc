#include <assert.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include <deque>
#include <string>
#include <thread>

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
#include "redis/src/ae.h"
#include "redismodule.h"
}

#include "glog/logging.h"
#include "grpcpp/grpcpp.h"
#include "leveldb/db.h"
#include "nlohmann/json.hpp";

#include "heartbeat_monitor.h"
#include "src/etcd/etcd_utils.h"
#include "src/utils.h"

using json = nlohmann::json;

extern "C" {
aeEventLoop* getEventLoop();
int getPort();
}

struct Member {
  std::string address;
  std::string port;
  redisContext* context;
};

void to_json(json& j, const Member& m) {
  j = json{{"address", m.address}, {"port", m.port}};
}

void from_json(const json& j, Member& m) {
  m.address = j.at("address").get<std::string>();
  m.port = j.at("port").get<std::string>();
  m.context = redisConnect(m.address.c_str(), std::stoi(m.port));
  if (m.context == NULL || m.context->err) {
    LOG(INFO) << "State in etcd contains unreachable member "
              << m.address + ":" + m.port;
  }
}

EtcdURL etcd_url;
std::unique_ptr<std::thread> heartbeat_monitor;
std::shared_ptr<grpc::Channel> etcd_channel;
std::deque<Member> members;

using Status = leveldb::Status;  // So that it can be easily replaced.

// TODO(zongheng): all callers of SetRole() in this module need to check the
// return status.

namespace {

// Handle failure for redis module command as caller, hiredis context as callee.
int ReplyIfFailure(RedisModuleCtx* rm_ctx,
                   redisContext* ctx,
                   redisReply* reply) {
  if (reply == NULL) {
    return RedisModule_ReplyWithError(rm_ctx, ctx->errstr);
  } else if (reply->type == REDIS_REPLY_ERROR) {
    return RedisModule_ReplyWithError(rm_ctx, reply->str);
  }
  return 0;
}

// Convert a redis reply to a grpc::Status for an unknown error code (since
// Redis errors are not represented 1-1 in grpc codes.)
grpc::Status ReplyToStatus(redisContext* ctx,
                           redisReply* reply) {
  if (reply == NULL) {
    return grpc::Status(grpc::StatusCode::UNKNOWN,
                        ctx->errstr);
  } else if (reply->type == REDIS_REPLY_ERROR) {
    return grpc::Status(grpc::StatusCode::UNKNOWN,
                        reply->str);
  }

  if (reply->type == REDIS_REPLY_STATUS || reply->type == REDIS_REPLY_STRING) {
    return grpc::Status(grpc::StatusCode::OK, reply->str);
  }

  if (reply->type == REDIS_REPLY_INTEGER) {
    return grpc::Status(grpc::StatusCode::OK, std::to_string(reply->integer));
  }

  // REDIS_REPLY_ARRAY is not covered because it requires recursive
  // unpacking and is currently not used by credis, so there's no point in
  // doing the extra work.
  return grpc::Status::OK;
}

}  // anonymous namespace

Status SetRole(redisContext* context,
               const std::string& role,
               const std::string& prev_address,
               const std::string& prev_port,
               const std::string& next_address,
               const std::string& next_port,
               long long* sn_result,
               long long sn = -1,
               long long drop_writes = 0) {
  const std::string sn_string = std::to_string(sn);
  const std::string drop_writes_string = std::to_string(drop_writes);
  if (!context->err) {
    redisReply* reply = reinterpret_cast<redisReply*>(redisCommand(
        context, "MEMBER.SET_ROLE %s %s %s %s %s %s %s", role.c_str(),
        prev_address.c_str(), prev_port.c_str(), next_address.c_str(),
        next_port.c_str(), sn_string.c_str(), drop_writes_string.c_str()));
    if (reply == NULL) {
      LOG(INFO) << "reply is NULL, IO error";
      LOG(INFO) << "error string: " << std::string(context->errstr);
      return Status::IOError(context->errstr);
    }

    LOG(INFO) << "Last sequence number is " << reply->integer;
    *sn_result = reply->integer;
    freeReplyObject(reply);
    return Status::OK();
  } else {
    LOG(INFO) << "ERROR: SetRole() cannot contact remote node, returning -1";
    LOG(INFO) << "error string: " << std::string(context->errstr);
    return Status::IOError(context->errstr);
  }
}

grpc::Status WriteChain(const std::deque<Member>& chain) {
  etcd3::Client etcd(etcd_channel);
  etcd3::pb::PutRequest request;
  request.set_key(etcd_url.chain_prefix + "/members");
  json j_members(chain);
  request.set_value(j_members.dump());
  etcd3::pb::PutResponse response;
  return etcd.Put(request, &response);
}

grpc::Status ReadChain(std::deque<Member>* chain) {
  etcd3::Client etcd(etcd_channel);
  etcd3::pb::RangeRequest request;
  request.set_key(etcd_url.chain_prefix + "/members");
  etcd3::pb::RangeResponse response;
  auto status = etcd.Range(request, &response);
  if (!status.ok()) {
    return status;
  }

  if (response.kvs_size() == 0) {
    return grpc::Status::OK;
  }

  std::deque<Member> remote_chain = json::parse(response.kvs().Get(0).value());
  *chain = remote_chain;

  return grpc::Status::OK;
}

// Remove the replica at ADDRESS:PORT from the chain.
// Returns a status that encapsulates the error.
grpc::Status MasterRemove_Impl(std::string address, std::string port) {
  // Find the node to be removed
  size_t index = 0;
  do {
    if (members[index].address == address && members[index].port == port) {
      break;
    }
    index += 1;
  } while (index < members.size());

  if (index == members.size()) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "replica not found");
  }

  // 3 cases. If the head or tail failed, it's simple to handle.
  bool head_failed = (index == 0);
  bool tail_failed = (index == members.size() - 1);

  members.erase(members.begin() + index);

  // Case: no nodes remain in the chain.
  if (members.size() == 0) {
    LOG(INFO) << "Removed the last member.";
    auto status = WriteChain(members);
    if (!status.ok()) {
      return status;
    }
    return grpc::Status::OK;
  }

  // Singleton case.
  long long sn = -1;
  if (members.size() == 1) {
    LOG(INFO) << "1 node left, setting it as SINGLETON.";
    SetRole(members[0].context, "singleton", "nil", "nil", "nil", "nil", &sn);
    auto status = WriteChain(members);
    if (!status.ok()) {
      return status;
    }
    return grpc::Status::OK;
  }

  // At least 2 nodes left.
  if (tail_failed) {
    LOG(INFO) << "Removed the tail.";
    SetRole(members[index - 1].context, "tail", "nil", "nil",
            members[0].address, members[0].port, &sn);
  } else if (head_failed) {
    LOG(INFO) << "Removed the head.";
    SetRole(members[0].context, "head", "nil", "nil", members[1].address,
            members[1].port, &sn);
  } else {
    // TODO: this case is incompletely handled.  See "Failure of Other Servers"
    // in the chain rep paper.
    LOG(INFO) << "Removed the middle node " << index << ".";
    SetRole(members[index].context, "", members[index - 1].address,
            members[index - 1].port, "", "", &sn);

    long long unused = -1;
    SetRole(members[index - 1].context, "", "", "", members[index].address,
            members[index].port, &unused, sn);

    LOG(INFO) << "Resending unacked updates.";
    redisReply* reply = reinterpret_cast<redisReply*>(
        redisCommand(members[index - 1].context, "MEMBER.REPLICATE"));
    auto status = ReplyToStatus(members[index - 1].context, reply);
    if (!status.ok()) {
      freeReplyObject(reply);
      return status;
    }
    freeReplyObject(reply);

    // Let writes flow through.
    reply = reinterpret_cast<redisReply*>(
        redisCommand(members[index - 1].context, "MEMBER.UNBLOCK_WRITES"));
    status = ReplyToStatus(members[index - 1].context, reply);
    if (!status.ok()) {
      freeReplyObject(reply);
      return status;
    }
    freeReplyObject(reply);
  }

  return WriteChain(members);
}

// Redis command to remove a replica.
// argv[1] is the IP address of the replica to be removed
// argv[2] is the port of the replica to be removed
int MasterRemove_RedisCommand(RedisModuleCtx* ctx,
                              RedisModuleString** argv,
                              int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }

  std::string address = ReadString(argv[1]);
  std::string port = ReadString(argv[2]);

  auto status = MasterRemove_Impl(address, port);
  if (!status.ok()) {
    return RedisModule_ReplyWithError(ctx, status.error_message().c_str());
  }
  return RedisModule_ReplyWithNull(ctx);
}

// Handling node addition at the end of chain.
//
// We follow the protocol outlined at the end of Section 3 of the paper.
// However, currently we drop the optimization that old tail concurrently
// process reads/writes while the state transfer is underway.  Here's the
// variant:
//
// MasterAdd()
//    (1) SetRole(old_tail, new_role=MIDDLE, next=new_tail, drop_writes=True)
//        i.e., old_tail.MemberSetRole()
//    (2) old_tail.MemberReplicate()
//    (3) SetRole(new_tail, new_role=TAIL, prev=old_tail)
//        i.e., new_tail.MemberSetRole()
//    (4) old_tail.UnblockWrites()
//
// After (1), the old tail will drop all write requests, and reads will fail
// too due to the role change.   Completion of (4) let new write requests flow
// through again, and free up the master to handle RefreshTail from the clients.

// Add a new replica to the chain
// argv[1] is the IP address of the replica to be added
// argv[2] is the port of the replica to be added
int MasterAdd_RedisCommand(RedisModuleCtx* ctx,
                           RedisModuleString** argv,
                           int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }

  const std::string address = ReadString(argv[1]);
  const std::string port = ReadString(argv[2]);

  // If member is already in the chain, remove it to be readded as the tail.
  for (auto member : members) {
    bool already_added = (member.address == address && member.port == port);
    if (already_added) {
      MasterRemove_RedisCommand(ctx, argv, argc);
    }
  }

  size_t size = members.size();
  redisContext* context = SyncConnect(address, std::stoi(port));
  if (size == 0) {
    LOG(INFO) << "First node joined.";
    // Fall through.
  } else {
    LOG(INFO) << "New tail node joined.";
    long long unused = -1;

    // We search for the first non-faulty member, starting from end of chain
    // towards the beginning.  This member becomes the parent of our new node.
    Status s;
    Member found_tail;
    while (size >= 1) {
      found_tail = members[size - 1];

      if (size > 1) {
        s = SetRole(found_tail.context, "middle",
                    /*re-use cached prev addr and port*/ "", "",
                    /*next addr and port*/ address, port, &unused, /*sn=*/-1,
                    /*drop_writes=*/1);
      } else {
        s = SetRole(found_tail.context, "head",
                    /*prev addr and port*/ "nil", "nil",
                    /*next addr and port*/ address, port, &unused, /*sn=*/-1,
                    /*drop_writes=*/1);
      }
      if (s.ok()) break;
      LOG(INFO) << "Member " << size - 1
                << " found dead, removing; err: " << s.ToString();
      --size;
      members.pop_back();
    }

    if (size > 0) {
      // TODO(pcm): Execute Sent_T requests
      LOG(INFO) << "Replicating the tail.";
      redisReply* reply = reinterpret_cast<redisReply*>(
          redisCommand(found_tail.context, "MEMBER.REPLICATE"));
      ReplyIfFailure(ctx, found_tail.context, reply);
      freeReplyObject(reply);

      LOG(INFO) << "Setting new tail.";
      CHECK(SetRole(context, "tail", found_tail.address, found_tail.port, "nil",
                    "nil", &unused)
                .ok());

      // Let writes flow through.
      reply = reinterpret_cast<redisReply*>(
          redisCommand(found_tail.context, "MEMBER.UNBLOCK_WRITES"));
      ReplyIfFailure(ctx, found_tail.context, reply);
      freeReplyObject(reply);

    } else {
      LOG(INFO) << "Previous nodes all dead, new node becomes a singleton...";
      CHECK(SetRole(context, "singleton", "nil", "nil", "nil", "nil", &unused)
                .ok());
    }
  }
  Member tail;
  tail.address = address;
  tail.port = port;
  tail.context = context;
  members.emplace_back(tail);

  auto status = WriteChain(members);
  if (!status.ok()) {
    return RedisModule_ReplyWithError(ctx, "Unable to write state to etcd.");
  }
  return RedisModule_ReplyWithNull(ctx);
}

// MASTER.REFRESH_HEAD: return the current head if non-faulty, otherwise
// designate the child of the old head as the new head.
//
// Returns, as a string, "<new head addr>:<new head port>".
int MasterRefreshHead_RedisCommand(RedisModuleCtx* ctx,
                                   RedisModuleString** argv,
                                   int argc) {
  REDISMODULE_NOT_USED(argv);
  if (argc != 1) {
    return RedisModule_WrongArity(ctx);
  }

  // RPC flow:
  // 1. master -> head: try to connect, check that if it's dead.
  // 2. (while dead) master cleans up state; then, try next member
  // 3. (on successful connection) return from this function the new head.

  CHECK(!members.empty());
  int head_index = 0;
  Member head = members.front();
  // (1).
  while (redisReconnect(head.context) != REDIS_OK) {
    members.pop_front();
    head = members[head_index];
  }
  // Current head is good.
  const std::string s = head.address + ":" + head.port;
  if (members.size() == 1) {
    LOG(INFO) << "SetRole(singleton)";
    long long unused = -1;
    SetRole(head.context, "singleton", "nil", "nil", "nil", "nil", &unused);
  } else {
    LOG(INFO) << "SetRole(head)";
    long long unused = -1;
    SetRole(head.context, "head", "nil", "nil", "", "", &unused);
  }
  auto status = WriteChain(members);
  if (!status.ok()) {
    return RedisModule_ReplyWithError(ctx, status.error_message().c_str());
  }
  return RedisModule_ReplyWithSimpleString(ctx, s.data());
}

// MASTER.REFRESH_TAIL: similar to MASTER.REFRESH_HEAD, but for getting the
// up-to-date tail.
int MasterRefreshTail_RedisCommand(RedisModuleCtx* ctx,
                                   RedisModuleString** argv,
                                   int argc) {
  REDISMODULE_NOT_USED(argv);
  if (argc != 1) {
    return RedisModule_WrongArity(ctx);
  }

  // RPC flow:
  // 1. master -> head: try to connect, check that if it's dead.
  // 2. (while dead) master cleans up state; then, try next member
  // 3. (on successful connection) return from this function the new head.

  CHECK(!members.empty());
  int head_index = 0;
  Member tail = members.back();
  // (1).
  while (redisReconnect(tail.context) != REDIS_OK) {
    members.pop_back();
    tail = members[head_index];
  }
  // Current head is good.
  const std::string s = tail.address + ":" + tail.port;
  if (members.size() == 1) {
    LOG(INFO) << "SetRole(singleton)";
    long long unused = -1;
    SetRole(tail.context, "singleton", "nil", "nil", "nil", "nil", &unused);
  } else {
    LOG(INFO) << "SetRole(tail)";
    long long unused = -1;
    SetRole(tail.context, "tail", "", "", "nil", "nil", &unused);
  }
  auto status = WriteChain(members);
  if (!status.ok()) {
    return RedisModule_ReplyWithError(ctx, status.error_message().c_str());
  }
  return RedisModule_ReplyWithSimpleString(ctx, s.data());
}

// Return the current view of the chain.
int MasterGetChain_RedisCommand(RedisModuleCtx* ctx,
                                RedisModuleString** argv,
                                int argc) {
  REDISMODULE_NOT_USED(argv);
  if (argc != 1) return RedisModule_WrongArity(ctx);

  auto status = ReadChain(&members);
  if (!status.ok()) {
    return RedisModule_ReplyWithError(ctx, "Unable to read state from etcd.");
  }
  RedisModule_ReplyWithArray(ctx, members.size());
  LOG(INFO) << "GetChain: " << members.size() << " members";
  for (const auto m : members) {
    const std::string mstr = m.address + ":" + m.port;
    LOG(INFO) << mstr;
    RedisModule_ReplyWithSimpleString(ctx, mstr.c_str());
  }
  return REDISMODULE_OK;
}

extern "C" {

int RedisModule_OnLoad(RedisModuleCtx* ctx,
                       RedisModuleString** argv,
                       int argc) {
  ::google::InitGoogleLogging("libmaster");

  REDISMODULE_NOT_USED(argc);
  REDISMODULE_NOT_USED(argv);

  // Init.  Must be at the top.
  if (RedisModule_Init(ctx, "MASTER", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  CHECK_GE(argc, 1) << "Usage: --loadmodule libetcd_master.so "
                       "ETCD_ADDR:ETCD_PORT/CHAIN_ID ...";
  // Command parsing.
  auto url_str = ReadString(argv[0]);
  etcd_url = SplitEtcdURL(url_str);
  etcd_channel =
      grpc::CreateChannel(etcd_url.address, grpc::InsecureChannelCredentials());
  if (etcd_url.chain_prefix == "/") {
    CHECK(false) << "No key name was given to store the chain! "
                 << "Supply a key name to the agent. "
                 << "e.g. --loadmodule libetcd_agent.so 127.0.0.1:1337/mychain";
  }

  // Load initial chain state.
  auto status = ReadChain(&members);
  CHECK(status.ok()) << "Could not read chain.";

  for (auto member: members) {
    if (redisReconnect(member.context) != REDIS_OK) {
      MasterRemove_Impl(member.address, member.port);
    }
  }

  if (RedisModule_Init(ctx, "MASTER", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "MASTER.ADD", MasterAdd_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "MASTER.REMOVE", MasterRemove_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "MASTER.REFRESH_HEAD",
                                MasterRefreshHead_RedisCommand, "write",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "MASTER.REFRESH_TAIL",
                                MasterRefreshTail_RedisCommand, "write",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "MASTER.GET_CHAIN",
                                MasterGetChain_RedisCommand, "readonly",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  heartbeat_monitor.reset(new std::thread(
      [](EtcdURL url, std::shared_ptr<grpc::Channel> channel) {
        LOG(INFO) << "Starting heartbeat monitor.";
        int own_port = getPort();
        HeartbeatMonitor monitor(channel, own_port);
        auto status = monitor.Monitor(url.chain_prefix);
        LOG(INFO) << "Heartbeat monitor exited.";
        if (!status.ok()) {
          LOG(WARNING) << status.error_message();
        }
      },
      etcd_url, etcd_channel));

  return REDISMODULE_OK;
}
}
