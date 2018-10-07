
#include <assert.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include <string>
#include <vector>

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
#include "redismodule.h"
}
#include "glog/logging.h"
#include "leveldb/db.h"

#include "utils.h"

// Global definition.  A redis-server binary that --loadmodule <module built
// from this file> will have access to the symbol/object, "module".  This must
// be supported by a modified module.c in redis/src tree that uses
// dlopen(path,RTLD_LAZY|RTLD_GLOBAL) to load modules.
//
// We provide this definition -- but we don't use it in anyway -- as a hack with
// the following rationale.  Suppose a user redis module wants to make use of
// credis.  Since credis (currently) functions with a libmaster server and
// several libmember servers, the most convenient way for that user is to have
// one of her existing servers --loadmodule libmaster, and the rest of her
// servers --loadmodule libmember.  The user may not wish to modify her code, so
// her redis module code will contain a reference to "extern RedisChainModule
// module", regardless of whether libmaster or libmember is supposed to be
// linked in. Therefore, here at libmaster we must also define this symbol in
// order for the user's loading to succeed.
#include "chain_module.h"
RedisChainModule module;  // Unused.

struct Member {
  std::string address;
  std::string port;
  redisContext* context;
};

std::vector<Member> members;

using Status = leveldb::Status;  // So that it can be easily replaced.

// TODO(zongheng): all callers of SetRole() in this module need to check the
// return status.

namespace {

// TODO(zongheng): caller of this function doesn't use it correctly.
// Handle failure for redis module command as caller, hiredis context as callee.
int ReplyIfFailure(RedisModuleCtx* rm_ctx, redisContext* ctx,
                   redisReply* reply) {
  if (reply == NULL) {
    CHECK(0);
    return RedisModule_ReplyWithError(rm_ctx, ctx->errstr);
  } else if (reply->type == REDIS_REPLY_ERROR) {
    CHECK(0);
    return RedisModule_ReplyWithError(rm_ctx, reply->str);
  } else if (reply->type == REDIS_REPLY_STATUS) {
    LOG(INFO) << std::string(reply->str, reply->len);
  } else if (reply->type == REDIS_REPLY_NIL) {
    // LOG(INFO) << "nil reply";
  } else if (reply->type == REDIS_REPLY_STRING) {
    LOG(INFO) << std::string(reply->str, reply->len);
  }
  return 0;
}

}  // anonymous namespace

Status SetRole(redisContext* context, const std::string& role,
               const std::string& prev_address, const std::string& prev_port,
               const std::string& next_address, const std::string& next_port,
               const std::string& password, long long* sn_result,
               long long sn = -1, long long drop_writes = 0) {
  const std::string sn_string = std::to_string(sn);
  const std::string drop_writes_string = std::to_string(drop_writes);
  if (!context->err) {
    redisReply* reply = reinterpret_cast<redisReply*>(
        redisCommand(context, "MEMBER.SET_ROLE %s %s %s %s %s %s %s %s",
                     role.c_str(), prev_address.c_str(), prev_port.c_str(),
                     next_address.c_str(), next_port.c_str(), password.c_str(),
                     sn_string.c_str(), drop_writes_string.c_str()));
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

// TODO(zongheng): as it's currently implemented, MemberReplicate is NOT
// synchronous at the moment, so this presents a race. E.g., state transfer is
// still underway to the new tail, but step (3) below has already issued namely
// telling the new tail to start serving reads.

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
// argv[3] is the optional password for the port
int MasterAdd_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                           int argc) {
  if (argc != 3 && argc != 4) {
    return RedisModule_WrongArity(ctx);
  }

  const std::string address = ReadString(argv[1]);
  const std::string port = ReadString(argv[2]);
  const std::string password = (argc == 4) ? ReadString(argv[3]) : "";

  size_t size = members.size();
  LOG(INFO) << "connecting...";
  redisContext* context = SyncConnect(address, std::stoi(port), password);
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
                    /*next addr and port*/ address, port, password, &unused,
                    /*sn=*/-1,
                    /*drop_writes=*/1);
      } else {
        s = SetRole(found_tail.context, "head",
                    /*prev addr and port*/ "nil", "nil",
                    /*next addr and port*/ address, port, password, &unused,
                    /*sn=*/-1,
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
      LOG(INFO) << "Replicating from old tail to the new tail.";
      LOG(INFO) << "old tail's port " << found_tail.context->tcp.port;
      redisReply* reply;
      reply = reinterpret_cast<redisReply*>(
          redisCommand(found_tail.context, "MEMBER.REPLICATE"));
      ReplyIfFailure(ctx, found_tail.context, reply);
      CHECK(reply->type == REDIS_REPLY_INTEGER);
      LOG(INFO) << "Reply of MEMBER.REPLICATE (duration in ms): "
                << reply->integer;
      freeReplyObject(reply);

      LOG(INFO) << "Setting new tail.";
      CHECK(SetRole(context, "tail", found_tail.address, found_tail.port, "nil",
                    "nil", password, &unused)
                .ok());

      // Let writes flow through.
      reply = reinterpret_cast<redisReply*>(
          redisCommand(found_tail.context, "MEMBER.UNBLOCK_WRITES"));
      ReplyIfFailure(ctx, found_tail.context, reply);
      freeReplyObject(reply);

    } else {
      LOG(INFO) << "Previous nodes all dead, new node becomes a singleton...";
      CHECK(SetRole(context, "singleton", "nil", "nil", "nil", "nil", password,
                    &unused)
                .ok());
    }
  }
  Member tail;
  tail.address = address;
  tail.port = port;
  tail.context = context;
  members.emplace_back(tail);
  return RedisModule_ReplyWithNull(ctx);
}

// // Remove a replica from the chain
// // argv[1] is the IP address of the replica to be removed
// // argv[2] is the port of the replica to be removed
// int MasterRemove_RedisCommand(RedisModuleCtx* ctx,
//                               RedisModuleString** argv,
//                               int argc) {
//   if (argc != 3) {
//     return RedisModule_WrongArity(ctx);
//   }

//   std::string address = ReadString(argv[1]);
//   std::string port = ReadString(argv[2]);

//   // Find the node to be removed
//   size_t index = 0;
//   do {
//     if (members[index].address == address && members[index].port == port) {
//       break;
//     }
//     index += 1;
//   } while (index < members.size());

//   if (index == members.size()) {
//     return RedisModule_ReplyWithError(ctx, "replica not found");
//   }

//   members.erase(members.begin() + index);

//   // Singleton case.
//   long long sn = -1;
//   if (members.size() == 1) {
//     LOG(INFO) << "1 node left, setting it as SINGLETON.";
//     SetRole(members[0].context, "singleton", "nil", "nil", "nil", "nil",
//     &sn); return RedisModule_ReplyWithNull(ctx);
//   }

//   // At least 2 nodes left.
//   if (index == members.size() - 1) {
//     LOG(INFO) << "Removed the tail.";
//     SetRole(members[index - 1].context, "tail", "nil", "nil",
//             members[0].address, members[0].port, &sn);
//   } else if (index == 0) {
//     LOG(INFO) << "Removed the head.";
//     SetRole(members[0].context, "head", "nil", "nil", members[1].address,
//             members[1].port, &sn);
//   } else {
//     // TODO: this case is incompletely handled.  See "Failure of Other
//     Servers"
//     // in the chain rep paper.
//     LOG(INFO) << "Removed the middle node " << index << ".";
//     SetRole(members[index].context, "", members[index - 1].address,
//             members[index - 1].port, "", "", &sn);
//     long long unused = -1;
//     SetRole(members[index - 1].context, "", "", "", members[index].address,
//             members[index].port, &unused, sn);
//   }
//   return RedisModule_ReplyWithNull(ctx);
// }

// TODO(zongheng): I think it's incorrect in Refresh{Head,Tail} to reconnect
// first.  We should enforce the contract that the Refresh calls are performed
// only when clients detect disconnects.  The calls should forcibly remove the
// head/tail.  Otherwise race conditions arise.
// TODO(zongheng): after a month of originally writing the above, I don't
// understand it anymore.

// MASTER.REFRESH_HEAD: return the current head if non-faulty, otherwise
// designate the child of the old head as the new head.
//
// Returns, as a string, "<new head addr>:<new head port>".
int MasterRefreshHead_RedisCommand(RedisModuleCtx* ctx,
                                   RedisModuleString** argv, int argc) {
  REDISMODULE_NOT_USED(argv);
  if (argc != 1 && argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  const std::string password = (argc == 2) ? ReadString(argv[1]) : "";

  // RPC flow:
  // 1. master -> head: try to connect, check that if it's dead.
  // 2. (if dead) master cleans up state; then, master -> child of head:
  //    SetRole(head).
  // 3. (on ack) return from this function the new head.

  CHECK(!members.empty());
  redisContext* head = members[0].context;
  // (1).
  if (SyncReconnect(head, password)) {
    // Current head is good.
    const std::string s = members[0].address + ":" + members[0].port;
    return RedisModule_ReplyWithSimpleString(ctx, s.data());
  }
  // (2).
  members.erase(members.begin());
  CHECK(members.size() >= 1);
  long long unused = -1;
  // TODO(zongheng): We should really find the first non-faulty member, instead
  // of blindly trying only 1 member (members[0]); see the handling in
  // MasterAdd.
  if (members.size() == 1) {
    LOG(INFO) << "SetRole(singleton)";
    SetRole(members[0].context, "singleton", "nil", "nil", "nil", "nil",
            password, &unused);
  } else {
    LOG(INFO) << "SetRole(head)";
    SetRole(members[0].context, "head", /*prev addr and port*/ "nil", "nil",
            /*re-use cached next addr and port*/ "", "", password, &unused);
  }
  // (3).
  const std::string s = members[0].address + ":" + members[0].port;
  return RedisModule_ReplyWithSimpleString(ctx, s.data());
}

// MASTER.REFRESH_TAIL: similar to MASTER.REFRESH_HEAD, but for getting the
// up-to-date tail.
int MasterRefreshTail_RedisCommand(RedisModuleCtx* ctx,
                                   RedisModuleString** argv, int argc) {
  REDISMODULE_NOT_USED(argv);
  if (argc != 1 && argc != 2) {
    return RedisModule_WrongArity(ctx);
  }

  const std::string password = (argc == 2) ? ReadString(argv[1]) : "";

  // RPC flow is similar to MASTER.REFRESH_HEAD.
  CHECK(!members.empty());
  redisContext* tail = members.back().context;
  // (1).
  if (SyncReconnect(tail, password)) {
    // Current tail is good.
    const std::string s = members.back().address + ":" + members.back().port;
    return RedisModule_ReplyWithSimpleString(ctx, s.data());
  }
  // (2).
  members.pop_back();
  CHECK(members.size() >= 1);
  // TODO(zongheng): We should really find the first non-faulty member, instead
  // of blindly trying only 1 member (members.back()); see the handling in
  // MasterAdd.
  long long unused = -1;
  if (members.size() == 1) {
    LOG(INFO) << "SetRole(singleton)";
    SetRole(members.back().context, "singleton", "nil", "nil", "nil", "nil",
            password, &unused);
  } else {
    LOG(INFO) << "SetRole(tail)";
    SetRole(members.back().context, "tail",
            /*re-use cached prev addr and port*/ "", "",
            /*next addr and port*/ "nil", "nil", password, &unused);
  }
  // (3).
  const std::string s = members.back().address + ":" + members.back().port;
  LOG(INFO) << "Returning from MasterRefreshTail_RedisCommand: " << s;
  return RedisModule_ReplyWithSimpleString(ctx, s.data());
}

// Return the current view of the chain.
int MasterGetChain_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                                int argc) {
  REDISMODULE_NOT_USED(argv);
  if (argc != 1) return RedisModule_WrongArity(ctx);

  RedisModule_ReplyWithArray(ctx, members.size());
  LOG(INFO) << "GetChain: " << members.size() << " members";
  for (const auto m : members) {
    const std::string mstr = m.address + ":" + m.port;
    LOG(INFO) << mstr;
    RedisModule_ReplyWithSimpleString(ctx, mstr.c_str());
  }
  return REDISMODULE_OK;
}

int MasterTest_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                            int argc) {
  if (argc != 1) {
    return RedisModule_WrongArity(ctx);
  }
  LOG(INFO) << "MasterTest";

  // A libmember.so command.
  RedisModuleCallReply* reply = RedisModule_Call(ctx, "MEMBER.SN", "");

  CHECK(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_INTEGER)
      << "reply type: " << RedisModule_CallReplyType(reply);
  LOG(INFO) << "MEMBER.SN reply: " << RedisModule_CallReplyInteger(reply);
  return RedisModule_ReplyWithLongLong(ctx,
                                       RedisModule_CallReplyInteger(reply));

  // CHECK(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR);
  // return RedisModule_ReplyWithCallReply(ctx, reply);
}

extern "C" {

int RedisModule_OnLoad(RedisModuleCtx* ctx, RedisModuleString** argv,
                       int argc) {
  FLAGS_logtostderr = 1;  // By default glog uses log files in /tmp.
  ::google::InitGoogleLogging("libmaster");

  REDISMODULE_NOT_USED(argc);
  REDISMODULE_NOT_USED(argv);

  if (RedisModule_Init(ctx, "MASTER", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "MASTER.ADD", MasterAdd_RedisCommand,
                                "write", 1, 1, 1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  // if (RedisModule_CreateCommand(ctx, "MASTER.REMOVE",
  // MasterRemove_RedisCommand,
  //                               "write", 1, 1, 1) == REDISMODULE_ERR) {
  //   return REDISMODULE_ERR;
  // }
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
  if (RedisModule_CreateCommand(ctx, "MASTER.TEST", MasterTest_RedisCommand,
                                "readonly",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
}
