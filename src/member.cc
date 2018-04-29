#include "chain_module.h"

#include <assert.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include <cstdlib>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

extern "C" {
#include "hiredis/adapters/ae.h"
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
#include "redis/src/ae.h"
#include "redis/src/redismodule.h"
}
#include "glog/logging.h"
#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#include "timer.h"
#include "utils.h"

Timer timer;

extern "C" {
aeEventLoop* getEventLoop();
}

// TODO(zongheng): whenever we are about to do redisAsyncCommand(remote, ...),
// we should test for remote->err first.  See the NOTE in Put().

// Global definition.  A redis-server binary that --loadmodule <module built
// from this file> will have access to the symbol/object, "module".  This must
// be supported by a modified module.c in redis/src tree that uses
// dlopen(path,RTLD_LAZY|RTLD_GLOBAL) to load modules.
RedisChainModule module;

namespace {

int DoFlush(RedisModuleCtx* ctx, int64_t sn_left, int64_t sn_right,
            int64_t sn_ckpt, const std::vector<std::string>& flushable_keys) {
  CHECK(module.gcs_mode() == RedisChainModule::GcsMode::kCkptFlush);

  const int64_t old = module.key_to_sn().size();

  for (const std::string& key : flushable_keys) {
    RedisModuleString* rms =
        RedisModule_CreateString(ctx, key.data(), key.size());
    RedisModuleKey* rmkey = reinterpret_cast<RedisModuleKey*>(
        RedisModule_OpenKey(ctx, rms, REDISMODULE_READ | REDISMODULE_WRITE));
    CHECK(REDISMODULE_OK == RedisModule_DeleteKey(rmkey));
    RedisModule_CloseKey(rmkey);
    RedisModule_FreeString(ctx, rms);

    // NOTE(zongheng): DEL seems to have some weird issues.  See below.
    // RedisModuleCallReply* reply =
    //     RedisModule_Call(ctx, "DEL", "c", key.c_str());
    // CHECK(reply != NULL);
    // CHECK(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_INTEGER);
    // if (1 != RedisModule_CallReplyInteger(reply)) {
    //   DLOG(INFO) << "Failed to delete key " << key << " with response code "
    //              << RedisModule_CallReplyInteger(reply) << "; module's sn "
    //              << module.sn() << " sn_left " << sn_left << " sn_right "
    //              << sn_right << " sn_ckpt " << sn_ckpt;
    //   reply = RedisModule_Call(ctx, "INFO", "c", "all");
    //   CHECK(reply != NULL);
    //   DLOG(INFO) << "reply type " << RedisModule_CallReplyType(reply);
    //   // CHECK(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING);
    //   size_t len;
    //   char* ptr =
    //       const_cast<char*>(RedisModule_CallReplyStringPtr(reply, &len));
    //   DLOG(INFO) << std::string(ptr, len);

    //   // Try reading the key.
    //   char* key_ptr = RedisModule_StringDMA(rmkey, &len, REDISMODULE_READ);
    //   DLOG(INFO) << "key_ptr " << key_ptr << " len " << len;
    //   DLOG(INFO) << "  string form " << std::string(key_ptr, len);

    //   reply = RedisModule_Call(ctx, "GET", "c", key.c_str());
    //   CHECK(reply != NULL);
    //   DLOG(INFO) << "reply type " << RedisModule_CallReplyType(reply);
    //   // CHECK(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING);
    //   ptr = const_cast<char*>(RedisModule_CallReplyStringPtr(reply, &len));

    //   DLOG(INFO) << "  GET " << key << ": " << std::string(ptr, len);
    // }

    // Clean up key_to_sn.
    module.key_to_sn().erase(key);
  }

  const int64_t diff = old - module.key_to_sn().size();
  DLOG(INFO) << "Erased from key_to_sn " << diff << " entries; old " << old
             << " new " << module.key_to_sn().size() << "; sn_right-sn_left "
             << sn_right - sn_left << " flushable_keys.size "
             << flushable_keys.size();

  // Clean up sn_to_key; all seqnum < sn_right is removed.  We do
  // this even if flushable_keys is empty.
  module.CleanUpSnToKeyLessThan(sn_right);

  // Done, or propagate.  We do this even if flushable_keys is empty.
  if (module.ActAsTail()) {
    Status s = module.Master()->SetWatermark(
        MasterClient::Watermark::kSnFlushed, sn_right);
    HandleNonOk(ctx, s);
  } else {
    // Propagate: Child.MemberFlush(...).
    const std::string sn_left_s = std::to_string(sn_left);
    const std::string sn_right_s = std::to_string(sn_right);
    const std::string sn_ckpt_s = std::to_string(sn_ckpt);
    int status = redisAsyncCommand(module.child(), NULL, NULL,
                                   "_MEMBER.FLUSH %s %s %s", sn_left_s.data(),
                                   sn_right_s.data(), sn_ckpt_s.data());
    // TODO(zongheng): check status.
  }
  return RedisModule_ReplyWithLongLong(ctx, flushable_keys.size());
}

// Collect all keys with sn in [sn_left, sn_right), and that are not dirty
// w.r.t. sn_ckpt.
void CollectFlushableKeys(int64_t sn_left, int64_t sn_right, int64_t sn_ckpt,
                          std::vector<std::string>* flushable_keys) {
  const auto& sn_to_key = module.sn_to_key();
  const auto& key_to_sn = module.key_to_sn();

  for (int64_t seqnum = sn_left; seqnum < sn_right; ++seqnum) {
    const auto it = sn_to_key.find(seqnum);
    CHECK(it != sn_to_key.end());
    const std::string& key_str(it->second);
    const auto it_keytosn = key_to_sn.find(key_str);

    if (it_keytosn == key_to_sn.end()) {
      VLOG(2) << "key_to_sn has no record of key '" << key_str << "'";
      // It's already flushed.  DoFlush() below simply removes this sn_to_key
      // entry down the chain.
    } else if (it_keytosn->second < sn_ckpt) {
      VLOG(2) << "key_to_sn recorded key '" << key_str
              << "' with its latest sn " << it_keytosn->second;
      // Key is not dirty with respect to the checkpoint watermark: flushable.
      // The comparison is < not <=, since "sn_ckpt" points to smallest _yet_
      // to be checkpointed seqnum.
      flushable_keys->push_back(std::move(key_str));
    } else {
      // Key is dirty, will be flushed later.
    }
  }
  DLOG(INFO) << " #flushable_keys " << flushable_keys->size();
}

// Helper function to handle updates locally.
//
// For all nodes: (1) actual update into redis, (1) update the internal
// sn_to_key state.
//
// For non-tail nodes: propagate.
//
// For tail: publish that the request has been finalized, and ack up to the
// chain.
int Put(RedisModuleCtx* ctx, RedisModuleString* name, RedisModuleString* data,
        RedisModuleString* client_id, long long sn) {
  RedisModuleKey* key = reinterpret_cast<RedisModuleKey*>(
      RedisModule_OpenKey(ctx, name, REDISMODULE_WRITE));
  CHECK(REDISMODULE_OK == RedisModule_StringSet(key, data))
      << "key " << key << " sn " << sn;
  RedisModule_CloseKey(key);

  // State maintenance.
  //
  // Update sn_to_key (for all execution modes; used for node addition codepath)
  // and optionally key_to_sn (when flushing is on).
  const std::string key_str(ReadString(name));
  // NOTE(zongheng): this can be slow, see the note in class declaration.
  module.sn_to_key()[sn] = key_str;
  if (module.gcs_mode() == RedisChainModule::GcsMode::kCkptFlush) {
    module.key_to_sn()[key_str] = sn;
  }
  module.record_sn(static_cast<int64_t>(sn));

  const std::string seqnum_str = std::to_string(sn);

  // Protocol.
  if (module.ActAsTail()) {
    // LOG(INFO) << "sn_string published " << sn_string;
    //    RedisModuleCallReply* reply =
    //        RedisModule_Call(ctx, "PUBLISH", "sc", client_id,
    //        sn_string.c_str());
    //    if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
    //      return RedisModule_ReplyWithCallReply(ctx, reply);
    //    }

    RedisModuleString* s =
        RedisModule_CreateString(ctx, seqnum_str.data(), seqnum_str.size());
    RedisModule_Publish(client_id, s);
    RedisModule_FreeString(ctx, s);

    // if (module.parent()) {
    //  RedisModuleCallReply* reply =
    //      RedisModule_Call(ctx, "MEMBER.ACK", "c", seqnum_str.c_str());
    //  if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
    //    return RedisModule_ReplyWithCallReply(ctx, reply);
    //  }
    //}
  } else {
    // NOTE: here we do redisAsyncCommand(child, ...).  However, if the child
    // crashed before the call, this function non-deterministically crashes
    // with, say, a single digit percent chance.  We guard against this by
    // testing the "err" field first.
    // LOG_EVERY_N(INFO, 999999999) << "Calling MemberPropagate_RedisCommand";
    if (!module.child()->err) {
      // Zero-copy.
      size_t key_len = 0;
      const char* key_ptr = ReadString(name, &key_len);
      size_t val_len = 0;
      const char* val_ptr = ReadString(data, &val_len);
      size_t cid_len = 0;
      const char* cid_ptr = ReadString(client_id, &cid_len);

      const int status = redisAsyncCommand(
          module.child(), NULL, NULL, "MEMBER.PROPAGATE %b %b %b %b", key_ptr,
          key_len, val_ptr, val_len, seqnum_str.data(), seqnum_str.size(),
          cid_ptr, cid_len);
      // TODO(zongheng): check status.
      // LOG_EVERY_N(INFO, 999999999) << "Done";
      // module.sent().insert(sn);
    } else {
      // TODO(zongheng): this case is incompletely handled, i.e. "failure of a
      // middle server".  To handle this the Sent list data structure needs to
      // be extended to include an executable representation of the sent
      // commands.
      // LOG_EVERY_N(INFO, 999999999)
      //     << "Child dead, waiting for master to intervene.";
      // LOG_EVERY_N(INFO, 999999999)
      //     << "Redis context error: '" <<
      //     std::string(module.child()->errstr)
      //     << "'.";
      // TODO(zongheng): is it okay to reply SN to client in this case as
      // well?
    }
  }
  // TODO: is this needed?!
  // Return the sequence number
  // if (!module.ActAsHead()) {RedisModule_ReplyWithNull(ctx);}
  // RedisModule_ReplyWithLongLong(ctx,sn);
  return REDISMODULE_OK;
}

// Find the position of next occurence of "c" starting from "str[pos]".  If not
// found, return std::string::npos.
size_t FindFirstOf(char c, size_t pos, const char* str, size_t len) {
  while (pos < len && str[pos] != c) {
    ++pos;
  }
  return pos < len ? pos : std::string::npos;
}

}  // namespace

// Set the role, successor and predecessor of this server.
// Each of the arguments can be the empty string, in which case it is not set.
//
// argv[1] is the role of this instance ("singleton", "head", "middle", "tail")
// argv[2] is the address of the previous node in the chain
// argv[3] is the port of the previous node in the chain
// argv[4] is the address of the next node in the chain
// argv[5] is the port of the next node in the chain
// argv[6] is the last sequence number the next node did receive (on node
//     removal) and -1 if no node is removed
// argv[7]: drop_writes?
// Returns the latest sequence number on this node
int MemberSetRole_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                               int argc) {
  if (argc != 8) {
    return RedisModule_WrongArity(ctx);
  }
  std::string role = ReadString(argv[1]);
  if (role == "singleton") {
    module.SetRole(RedisChainModule::ChainRole::kSingleton);
  } else if (role == "head") {
    module.SetRole(RedisChainModule::ChainRole::kHead);
  } else if (role == "middle") {
    module.SetRole(RedisChainModule::ChainRole::kMiddle);
  } else if (role == "tail") {
    module.SetRole(RedisChainModule::ChainRole::kTail);
  } else {
    CHECK(role == "");
  }

  std::string prev_address = ReadString(argv[2]);
  if (prev_address == "") {
    prev_address = module.prev_address();
  }
  std::string prev_port = ReadString(argv[3]);
  if (prev_port == "") {
    prev_port = module.prev_port();
  }
  std::string next_address = ReadString(argv[4]);
  if (next_address == "") {
    next_address = module.next_address();
  }
  std::string next_port = ReadString(argv[5]);
  if (next_port == "") {
    next_port = module.next_port();
  }

  DLOG(INFO) << "MemberSetRole args: " << role << " " << prev_address << " "
             << prev_port << " " << next_address << " " << next_port;

  module.Reset(prev_address, prev_port, next_address, next_port);

  if (module.child()) {
    CHECK(!module.child()->err);
    aeEventLoop* loop = getEventLoop();
    redisAeAttach(loop, module.child());
  }

  if (module.parent()) {
    CHECK(!module.parent()->err);
    aeEventLoop* loop = getEventLoop();
    redisAeAttach(loop, module.parent());
  }

  const int64_t first_sn = std::stoi(ReadString(argv[6]));
  const bool drop_writes = std::stoi(ReadString(argv[7])) ? true : false;
  LOG(INFO) << "In MemberSetRole drop_writes: " << drop_writes;
  if (drop_writes) module.SetDropWrites(drop_writes);

  // TODO(zongheng): I don't understand why we do this.
  if (module.child()) {
    for (auto i = module.sn_to_key().find(first_sn);
         i != module.sn_to_key().end(); ++i) {
      std::string sn = std::to_string(i->first);
      std::string key = i->second;
      KeyReader reader(ctx, key);
      size_t size;
      const char* value = reader.value(&size);
      if (!module.child()->err) {
        const int status = redisAsyncCommand(
            module.child(), NULL, NULL, "MEMBER.PROPAGATE %b %b %b", key.data(),
            key.size(), value, size, sn.data(), sn.size());
        // TODO(zongheng): check status.
      }
    }
  }

  // LOG_EVERY_N(INFO, 999999999)
  //     << "Called SET_ROLE with role " << module.ChainRoleName()
  //     << " and addresses " << prev_address << ":" << prev_port << " and "
  //     << next_address << ":" << next_port;

  RedisModule_ReplyWithLongLong(ctx, module.sn());
  return REDISMODULE_OK;
}

int MemberConnectToMaster_RedisCommand(RedisModuleCtx* ctx,
                                       RedisModuleString** argv, int argc) {
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  size_t size = 0;
  const char* ptr = RedisModule_StringPtrLen(argv[1], &size);
  long long port = 0;
  RedisModule_StringToLongLong(argv[2], &port);
  Status s = module.ConnectToMaster(std::string(ptr, size), port);
  if (!s.ok()) return RedisModule_ReplyWithError(ctx, s.ToString().data());
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

// Put a key. This is only called on the head node by the client.
// argv[1] is the key for the data
// argv[2] is the data
// argv[3] is unique client id
int MemberPut_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                           int argc) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  DLOG(INFO) << "MemberPut, drop_writes? " << module.DropWrites()
             << "; act_as_head? " << module.ActAsHead();
  if (module.ActAsHead()) {
    // LOG(INFO) << "MemberPut";
    if (!module.DropWrites()) {
      const long long sn = module.inc_sn();
      DLOG(INFO) << "Returning assigned sn: " << sn;
      // Return the sequence number
      RedisModule_ReplyWithLongLong(ctx, sn);

      // TODO(zongheng): which one is faster?
      // const std::string sn_string = std::to_string(sn);
      // RedisModule_ReplyWithStringBuffer(ctx, sn_string.data(),
      //                                  sn_string.size());

      // LOG(INFO) << "MemberPut, assigning new sn " << sn;
      return Put(ctx, argv[1], argv[2], argv[3], sn);
    } else {
      // The store, by contract, is allowed to ignore writes during faults.

      // Returning nothing seems to be a better choice than returning a special
      // value like -1, as in the future we might consider shifting the burden
      // of generating seqnum to the client.  Under that setting, I think the
      // client should be able to generate arbitrary seqnums, not just
      // nonnegative numbers.  Although, hiredis / redis might have
      // undiscovered issues with a redis module command not replying
      // anything...
      //
      // Note, though, we should not do the following:
      //     return RedisModule_ReplyWithNull(ctx);
      // as the redis protocol will translate this into the equivalent of
      // returning a normal long long with value 0.  This could be confusing
      // since a seqnum of 0 appears to be a valid reply to the client.

      // return RedisModule_ReplyWithLongLong(ctx, -1);
      return REDISMODULE_OK;
    }
  } else {
    return RedisModule_ReplyWithError(ctx, "ERR called PUT on non-head node");
  }
}

int NoReply_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                         int argc) {
  if (argc != 1) {
    return RedisModule_WrongArity(ctx);
  }
  LOG(INFO) << "Processed NoReply";
  return 0;  // STATUS_OK
}

// Propagate a put request down the chain
// argv[1] is the key for the data
// argv[2] is the data
// argv[3] is the sequence number for this update request
// argv[4] is unique client id
int MemberPropagate_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                                 int argc) {
  if (argc != 5) {
    return RedisModule_WrongArity(ctx);
  }
  if (!module.DropWrites()) {
    long long sn = -1;
    RedisModule_StringToLongLong(argv[3], &sn);
    return Put(ctx, argv[1], argv[2], argv[4], sn);
  } else {
    // The store, by contract, is allowed to ignore writes during faults.
    return RedisModule_ReplyWithNull(ctx);
    // return RedisModule_ReplyWithError(ctx,
    //                                   "Server set to drop_writes mode,
    //                                   retry?");
  }
}

// Put a key.  No propagation.

// This command is used when ME is being added as the new tail of the whole
// chain -- i.e., fill the contents of ME.  The caller is MemberReplicate.

// argv[1]: key
// argv[2]: val
// argv[3]: sn
int MemberNoPropPut_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                                 int argc) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  long long sn = -1;
  RedisModule_StringToLongLong(argv[3], &sn);
  CHECK(!module.DropWrites());
  CHECK(sn >= 0);

  // Redis state.
  // No propagation: just state maintenance.
  RedisModuleKey* key = reinterpret_cast<RedisModuleKey*>(
      RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE));
  RedisModule_StringSet(key, argv[2]);
  RedisModule_CloseKey(key);

  // Chain metadata state.
  module.sn_to_key()[sn] = ReadString(argv[1]);
  module.record_sn(static_cast<int64_t>(sn));

  return RedisModule_ReplyWithNull(ctx);
}

// Put a list of keys.  No propagation.
//
// This command is used when ME is being added as the new tail of the whole
// chain -- i.e., fill the contents of ME.  The caller is MemberReplicate.
//
// argv[1]: num_entries
// argv[2]: blob; see MemberReplicate_RedisCommand for its schema
int MemberNoPropBatchedPut_RedisCommand(RedisModuleCtx* ctx,
                                        RedisModuleString** argv, int argc) {
  const double start = timer.NowMicrosecs();
  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  long long num_entries = 0;
  RedisModule_StringToLongLong(argv[1], &num_entries);
  CHECK(num_entries >= 0);

  size_t blob_size = 0;
  const char* blob = RedisModule_StringPtrLen(argv[2], &blob_size);
  LOG(INFO) << "MemberNoPropBatchedPut_RedisCommand, num_entries "
            << num_entries << " blob_size " << blob_size;

  size_t pos = 0, next_pos = 0;
  while (num_entries > 0) {
    --num_entries;
    // Key.
    next_pos = FindFirstOf(' ', pos, blob, blob_size);
    CHECK(next_pos != std::string::npos);

    RedisModuleString* key_rms =
        RedisModule_CreateString(ctx, blob + pos, next_pos - pos);
    const std::string key_str(blob + pos, next_pos - pos);
    RedisModuleKey* key = reinterpret_cast<RedisModuleKey*>(
        RedisModule_OpenKey(ctx, key_rms, REDISMODULE_WRITE));
    DLOG(INFO) << key_str;

    // Val.
    pos = next_pos + 1;
    next_pos = FindFirstOf(' ', pos, blob, blob_size);
    CHECK(next_pos != std::string::npos);

    // Mutate redis state.
    RedisModuleString* val_rms =
        RedisModule_CreateString(ctx, blob + pos, next_pos - pos);
    RedisModule_StringSet(key, val_rms);

    RedisModule_FreeString(ctx, val_rms);
    RedisModule_CloseKey(key);
    RedisModule_FreeString(ctx, key_rms);

    // Seqnum.
    pos = next_pos + 1;
    next_pos = FindFirstOf(' ', pos, blob, blob_size);
    if (num_entries > 0) {
      CHECK(next_pos != std::string::npos);
    } else {
      CHECK(next_pos == std::string::npos);
    }
    const int64_t sn = std::strtoll(blob + pos, nullptr, /*base=*/10);
    DLOG(INFO) << sn;

    // Chain metadata state.
    module.sn_to_key()[sn] = std::move(key_str);
    // TODO(zongheng): this can probably be called once, if we can guarantee
    // the input blob is sorted by seqnum order.
    module.record_sn(static_cast<int64_t>(sn));

    // Exit iter.
    pos = next_pos + 1;
  }

  const double end = timer.NowMicrosecs();
  LOG(INFO) << "MemberNoPropBatchedPut took " << (end - start) / 1e3 << " ms";
  return REDISMODULE_OK;
}

// Replicate our content to our child.  This command is used when a node is
// being added as the new tail of the whole chain.

// NOTE: with Sent_T handling this can be asynchronous (in the tail-add path);
// however without handling Sent_T this function should probably be
// synchronous.
// TODO(zongheng): fix the note above; correct iff synchronous...
int MemberReplicate_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                                 int argc) {
  const double start = timer.NowMicrosecs();
  LOG(INFO) << "In MemberReplicate, argc " << argc;
  // REDISMODULE_NOT_USED(argv);
  // if (argc != 1) {
  //   return RedisModule_WrongArity(ctx);
  // }

  DLOG(INFO) << "has child? " << (module.child() != nullptr);
  // TODO(zongheng): we should cap the size of each send.
  if (module.child() && !module.sn_to_key().empty()) {
    DLOG(INFO) << "Called replicate. sn_to_key size "
               << module.sn_to_key().size();

    const size_t num_entries = module.sn_to_key().size();
    const std::string num_entries_str = std::to_string(num_entries);

    // Schema of blob:
    //   <key> _ <val> _ <sn> [_ <key> _ <value> _ <sn>]*
    // where "_" denotes a single empty space.
    // The receiver should parse the blob.
    //
    // Schema of the whole command:
    //   MEMBER.NO_PROP_BATCHED_PUT <num_entries> <blob>

    std::stringstream ss;
    int cnt = 0;
    // NOTE(zongheng): we basically use "sn_to_key" to iterate through
    // in-memory state for convenience only.  Presumably, we can rely on some
    // other mechanisms, such as redis' native iterator, to do this.
    for (auto element : module.sn_to_key()) {
      KeyReader reader(ctx, element.second);
      size_t key_size, value_size;
      const char* key_data = reader.key(&key_size);
      const char* value_data = reader.value(&value_size);
      const std::string sn = std::to_string(element.first);
      ss << key_data << " " << value_data << " " << sn;
      if (cnt + 1 < num_entries) {
        ss << " ";
      }
      ++cnt;
    }
    const std::string blob = ss.str();
    LOG(INFO) << "num_entries " << num_entries << " blob.size() " << blob.size()
              << " or " << blob.size() / (1 << 20) << " MB";

    // TODO(zongheng): for now this should probably be synchronous.
    const int status = redisAsyncCommand(
        module.child(), NULL, NULL, "MEMBER.NO_PROP_BATCHED_PUT %b %b",
        num_entries_str.data(), num_entries_str.size(), blob.data(),
        blob.size());
    CHECK(status == REDIS_OK);
  }
  const double end = timer.NowMicrosecs();
  const int millis = static_cast<int>((end - start) / 1e3);
  LOG(INFO) << "MemberReplicate took " << millis << " ms";
  return RedisModule_ReplyWithLongLong(ctx, millis);
}

// Send ack up the chain.
// This could be batched in the future if we want to.
// argv[1] is the sequence number that is acknowledged
int MemberAck_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                           int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  std::string sn = ReadString(argv[1]);
  // LOG_EVERY_N(INFO, 999999999)
  //     << "Erasing sequence number " << sn << " from sent list";
  // module.sent().erase(std::stoi(sn));
  if (module.parent()) {
    // LOG_EVERY_N(INFO, 999999999) << "Propagating the ACK up the chain";
    const int status = redisAsyncCommand(module.parent(), NULL, NULL,
                                         "MEMBER.ACK %b", sn.data(), sn.size());
    // TODO(zongheng): check status.
  }
  // TODO: is this needed?
  // RedisModule_ReplyWithNull(ctx);
  return REDISMODULE_OK;
}

// Let new writes flow through.
// Used in the node addition code path (see MASTER.ADD).
int MemberUnblockWrites_RedisCommand(RedisModuleCtx* ctx,
                                     RedisModuleString** argv, int argc) {
  REDISMODULE_NOT_USED(argv);
  if (argc != 1) return RedisModule_WrongArity(ctx);
  LOG(INFO) << "Unblocking writes";
  module.SetDropWrites(false);
  return RedisModule_ReplyWithNull(ctx);
}

// TODO(zongheng): make these configurable (in current form or otherwise).
const int kMaxEntriesToCkptOnce = 350000;
const int kMaxEntriesToFlushOnce = 350000;
// const int kMaxEntriesToCkptOnce = 1 << 30;
// const int kMaxEntriesToFlushOnce = 1 << 30;

// TAIL.CHECKPOINT: incrementally checkpoint in-memory entries to durable
// storage.
//
// Returns the number of sequence numbers newly checkpointed.  Errors out if
// not called on the tail.
int TailCheckpoint_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                                int argc) {
  const double start = timer.NowMicrosecs();
  REDISMODULE_NOT_USED(argv);
  if (argc != 1) {  // No arg needed.
    return RedisModule_WrongArity(ctx);
  }
  if (!module.ActAsTail()) {
    return RedisModule_ReplyWithError(
        ctx, "ERR this command must be called on the tail.");
  }
  if (module.gcs_mode() == RedisChainModule::GcsMode::kNormal) {
    return RedisModule_ReplyWithError(
        ctx, "ERR redis server's GcsMode is set to kNormal.");
  }

  // TODO(zongheng): the following checkpoints the range [sn_ckpt, sn_latest],
  // but any smaller chunk is valid (and perhaps desirable, when we want to
  // keep this redis command running < 1ms, say.)
  int64_t sn_ckpt = 0;
  DLOG(INFO) << "getting watermark";
  Status s =
      module.Master()->GetWatermark(MasterClient::Watermark::kSnCkpt, &sn_ckpt);
  DLOG(INFO) << "done getting watermark " << s.ToString();
  HandleNonOk(ctx, s);
  const int64_t sn_latest = module.sn();

  const int64_t sn_bound = std::min(sn_ckpt + kMaxEntriesToCkptOnce, sn_latest);

  // Fill in "redis key -> redis value".
  std::vector<std::string> keys_to_write;
  std::vector<std::string> vals_to_write;
  const auto& sn_to_key = module.sn_to_key();
  size_t size = 0;
  for (int64_t s = sn_ckpt; s <= sn_bound; ++s) {
    auto i = sn_to_key.find(s);
    CHECK(i != sn_to_key.end())
        << "ERR the sn_to_key map doesn't contain seqnum " << s;
    std::string key = i->second;
    const KeyReader reader(ctx, key);
    const char* value = reader.value(&size);

    keys_to_write.push_back(key);
    vals_to_write.emplace_back(std::string(value, size));
  }

  // Batch the writes.
  leveldb::WriteBatch batch;
  for (size_t i = 0; i < keys_to_write.size(); ++i) {
    batch.Put(keys_to_write[i], vals_to_write[i]);
  }
  // Open the checkpoint.
  leveldb::DB* ckpt;
  s = module.OpenCheckpoint(&ckpt);
  HandleNonOk(ctx, s);
  std::unique_ptr<leveldb::DB> ptr(ckpt);  // RAII.
  // TODO(zongheng): tune WriteOptions (sync, compression, etc).
  s = ckpt->Write(leveldb::WriteOptions(), &batch);
  HandleNonOk(ctx, s);

  DLOG(INFO) << "Sending new watermark sn_ckpt " << sn_bound + 1
             << " to master";
  s = module.Master()->SetWatermark(MasterClient::Watermark::kSnCkpt,
                                    sn_bound + 1);
  HandleNonOk(ctx, s);

  const int64_t num_checkpointed = sn_bound - sn_ckpt + 1;

  const double end = timer.NowMicrosecs();
  LOG(INFO) << "TailCheckpoint took " << (end - start) / 1e3 << " ms";

  return RedisModule_ReplyWithLongLong(ctx, num_checkpointed);
}

// HEAD.FLUSH: incrementally flush checkpointed entries out of memory.
//
// Replies to the client # of keys removed from redis state, including 0.
//
// Errors out if not called on the head, or if GcsMode is not kCkptFlush.
int HeadFlush_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                           int argc) {
  const double start = timer.NowMicrosecs();
  REDISMODULE_NOT_USED(argv);
  if (argc != 1) return RedisModule_WrongArity(ctx);  // No args needed.
  if (!module.ActAsHead()) {
    return RedisModule_ReplyWithError(
        ctx, "ERR this command must be called on the head.");
  }
  if (module.gcs_mode() != RedisChainModule::GcsMode::kCkptFlush) {
    return RedisModule_ReplyWithError(
        ctx, "ERR redis server's GcsMode is NOT set to kCkptFlush.");
  }

  // Read watermarks from master.
  // Clearly, we can provide a batch iface.
  int64_t sn_ckpt = 0, sn_flushed = 0;
  HandleNonOk(ctx, module.Master()->GetWatermark(
                       MasterClient::Watermark::kSnCkpt, &sn_ckpt));
  HandleNonOk(ctx, module.Master()->GetWatermark(
                       MasterClient::Watermark::kSnFlushed, &sn_flushed));
  DLOG(INFO) << "sn_flushed " << sn_flushed << " sn_ckpt " << sn_ckpt;

  // Any non-dirty keys in the range [sn_flushed, sn_ckpt) may be flushed.

  const int64_t sn_bound =
      std::min(sn_flushed + kMaxEntriesToFlushOnce, sn_ckpt);
  std::vector<std::string> flushable_keys;
  CollectFlushableKeys(sn_flushed, sn_bound, sn_ckpt, &flushable_keys);

  int result = DoFlush(ctx, /*sn_left*/ sn_flushed, /*sn_right*/ sn_bound,
                       /*sn_ckpt*/ sn_ckpt, flushable_keys);
  const double end = timer.NowMicrosecs();
  LOG(INFO) << "HeadFlush took " << (end - start) / 1e3 << " ms";
  return result;
}

// Internal command that propagates a flush.  Users should never call.
// Args, all required and are int:
//   <sn_left>  <sn_right>  <sn_ckpt>
int MemberFlush_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                             int argc) {
  if (argc != 4) return RedisModule_WrongArity(ctx);

  long long sn_left, sn_right, sn_ckpt;
  RedisModule_StringToLongLong(argv[1], &sn_left);
  RedisModule_StringToLongLong(argv[2], &sn_right);
  RedisModule_StringToLongLong(argv[3], &sn_ckpt);

  std::vector<std::string> flushable_keys;
  CollectFlushableKeys(sn_left, sn_right, sn_ckpt, &flushable_keys);
  DoFlush(ctx, sn_left, sn_right, sn_ckpt, flushable_keys);

  // TODO(zongheng): is this needed?  Do we want ACKs flying in-between
  // servers?
  return RedisModule_ReplyWithNull(ctx);
}

// LIST.CHECKPOINT: print to stdout all keys, values in the checkpoint file.
//
// For debugging.
int ListCheckpoint_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                                int argc) {
  REDISMODULE_NOT_USED(argv);
  if (argc != 1) {  // No arg needed.
    return RedisModule_WrongArity(ctx);
  }
  leveldb::DB* ckpt;
  Status s = module.OpenCheckpoint(&ckpt);
  HandleNonOk(ctx, s);
  std::unique_ptr<leveldb::DB> ptr(ckpt);  // RAII.
  // LOG_EVERY_N(INFO, 999999999) << "-- LIST.CHECKPOINT:";
  leveldb::Iterator* it = ckpt->NewIterator(leveldb::ReadOptions());
  std::unique_ptr<leveldb::Iterator> iptr(it);  // RAII.
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    if (it->key().ToString() == kCheckpointHeaderKey) {
      // Let's skip the special header for prettier printing.
      continue;
    }
    // LOG_EVERY_N(INFO, 999999999)
    //     << it->key().ToString() << ": " << it->value().ToString();
  }
  // LOG_EVERY_N(INFO, 999999999) << "-- Done.";
  HandleNonOk(ctx, it->status());
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

// READ: like redis' own GET, but can fall back to checkpoint file.
int Read_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv, int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  if (!module.ActAsTail()) {
    return RedisModule_ReplyWithError(
        ctx, "ERR this command must be called on the tail.");
  }

  KeyReader reader(ctx, argv[1]);
  if (!reader.IsEmpty()) {
    size_t size = 0;
    const char* value = reader.value(&size);
    return RedisModule_ReplyWithStringBuffer(ctx, value, size);
  } else if (module.gcs_mode() > RedisChainModule::GcsMode::kNormal) {
    // Fall back to checkpoint file.
    leveldb::DB* ckpt;
    Status s = module.OpenCheckpoint(&ckpt);
    HandleNonOk(ctx, s);
    std::unique_ptr<leveldb::DB> ptr(ckpt);  // RAII.

    size_t size = 0;
    const char* key = reader.key(&size);
    std::string value;
    s = ckpt->Get(leveldb::ReadOptions(), leveldb::Slice(key, size), &value);
    if (s.IsNotFound()) {
      return RedisModule_ReplyWithNull(ctx);
    }
    HandleNonOk(ctx, s);
    return RedisModule_ReplyWithStringBuffer(ctx, value.data(), value.size());
  } else {
    // No checkpoint file, so return nil signaling not found.
    return RedisModule_ReplyWithNull(ctx);
  }
}

// MEMBER.SN: the largest SN processed by this node.
//
// For debugging.
int MemberSn_RedisCommand(RedisModuleCtx* ctx, RedisModuleString** argv,
                          int argc) {
  REDISMODULE_NOT_USED(argv);
  if (argc != 1) {  // No arg needed.
    return RedisModule_WrongArity(ctx);
  }
  return RedisModule_ReplyWithLongLong(ctx, module.sn());
}

extern "C" {

// Optional argument:
//   argv[0] indicates the GcsMode: 0 for kNormal, 1 kCkptOnly, 2 kCkptFlush.
// If not specified, defaults to kNormal (checkpointing and flushing turned
// off).
//   argv[1] indicates the MasterType: 0 for redis (default), 1 for etcd
int RedisModule_OnLoad(RedisModuleCtx* ctx, RedisModuleString** argv,
                       int argc) {
  FLAGS_logtostderr = 1;  // By default glog uses log files in /tmp.
  ::google::InitGoogleLogging("libmember");

  // Init.  Must be at the top.
  if (RedisModule_Init(ctx, "MEMBER", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // Command parsing.
  long long gcs_mode = 0;
  if (argc > 0) {
    CHECK_EQ(REDISMODULE_OK, RedisModule_StringToLongLong(argv[0], &gcs_mode));
  }
  switch (gcs_mode) {
    case 0:
      module.set_gcs_mode(RedisChainModule::GcsMode::kNormal);
      break;
    case 1:
      module.set_gcs_mode(RedisChainModule::GcsMode::kCkptOnly);
      break;
    case 2:
      module.set_gcs_mode(RedisChainModule::GcsMode::kCkptFlush);
      break;
    default:
      return REDISMODULE_ERR;
  }
  LOG(INFO) << "GcsMode: " << module.gcs_mode_string();

  long long master_mode = 0;
  if (argc > 1) {
    CHECK_EQ(REDISMODULE_OK,
             RedisModule_StringToLongLong(argv[0], &master_mode));
  }
  switch (master_mode) {
    case 0:
      module.set_master_mode(RedisChainModule::MasterMode::kRedis);
      break;
    case 1:
      module.set_master_mode(RedisChainModule::MasterMode::kEtcd);
      break;
    default:
      return REDISMODULE_ERR;
  }
  LOG(INFO) << "GcsMode: " << module.gcs_mode_string();

  // Register all commands.

  if (RedisModule_CreateCommand(ctx, "MEMBER.SET_ROLE",
                                MemberSetRole_RedisCommand, "write", 1, 1,
                                1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "MEMBER.CONNECT_TO_MASTER",
                                MemberConnectToMaster_RedisCommand, "write",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // TODO(zongheng): This should be renamed: it's only ever called on head.
  if (RedisModule_CreateCommand(ctx, "MEMBER.PUT", MemberPut_RedisCommand,
                                "write pubsub", 1, 1, 1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // No propagation, no ACK, etc.  This is only used for the node addition
  // code path, and is not exposed to clients.
  if (RedisModule_CreateCommand(ctx, "MEMBER.NO_PROP_BATCHED_PUT",
                                MemberNoPropBatchedPut_RedisCommand, "write", 1,
                                1, 1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "MEMBER.NO_PROP_PUT",
                                MemberNoPropPut_RedisCommand, "write", 1, 1,
                                1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "MEMBER.PROPAGATE",
                                MemberPropagate_RedisCommand, "write pubsub", 1,
                                1, 1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "MEMBER.REPLICATE",
                                MemberReplicate_RedisCommand, "write pubsub", 1,
                                1, 1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "MEMBER.ACK", MemberAck_RedisCommand,
                                "write pubsub", 1, 1, 1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "MEMBER.UNBLOCK_WRITES",
                                MemberUnblockWrites_RedisCommand, "write",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(
          ctx, "READ", Read_RedisCommand,
          "readonly",  // TODO(zongheng): is this ok?  It opens the db.
          /*firstkey=*/-1, /*lastkey=*/-1,
          /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // Checkpointing & flushing.
  if (RedisModule_CreateCommand(ctx, "TAIL.CHECKPOINT",
                                TailCheckpoint_RedisCommand, "write",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "HEAD.FLUSH", HeadFlush_RedisCommand,
                                "write",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "_MEMBER.FLUSH", MemberFlush_RedisCommand,
                                "write",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  // Debugging only.
  if (RedisModule_CreateCommand(ctx, "LIST.CHECKPOINT",
                                ListCheckpoint_RedisCommand, "readonly",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "MEMBER.SN", MemberSn_RedisCommand,
                                "readonly",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  if (RedisModule_CreateCommand(ctx, "NOREPLY", NoReply_RedisCommand,
                                "readonly",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
}
