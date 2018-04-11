#ifndef CREDIS_CHAIN_MODULE_H_
#define CREDIS_CHAIN_MODULE_H_

#include <functional>
#include <map>
#include <set>
#include <unordered_map>

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
#include "redismodule.h"
}
#include "glog/logging.h"

#include "master_client.h"
#include "utils.h"

namespace {
const char* const kCheckpointPath =
    "/tmp/gcs_ckpt";  // TODO(zongheng): don't hardcode.
const char* const kCheckpointHeaderKey = "";

// Register this so that on disconnect, the respective redisAsyncContext will be
// timely freed by hiredis.  Otherwise non-deterministic crashes happen on next
// redisAsyncCommand() call.
void DisconnectCallback(const redisAsyncContext* /*context*/, int /*status*/) {
  // "context" will be freed by hiredis.  Quote: "The context object is always
  // freed after the disconnect callback fired."
}

redisAsyncContext* AsyncConnect(const std::string& address, int port) {
  redisAsyncContext* c = redisAsyncConnect(address.c_str(), port);
  if (c == NULL || c->err) {
    if (c) {
      LOG(INFO) << "Connection error: " << c->errstr;
      redisAsyncFree(c);
    } else {
      LOG(INFO) << "Connection error: can't allocate redis context";
    }
    return NULL;
  }
  redisAsyncSetDisconnectCallback(c, &DisconnectCallback);
  return c;
}

int HandleNonOk(RedisModuleCtx* ctx, Status s) {
  if (s.ok()) {
    return REDISMODULE_OK;
  }
  LOG(INFO) << s.ToString();
  RedisModule_ReplyWithSimpleString(ctx, "ERR");
  return REDISMODULE_ERR;
}

}  // namespace

using Status = leveldb::Status;  // So that it can be easily replaced.

class RedisChainModule {
 public:
  // Public enums.
  enum class ChainRole : int {
    // 1-node chain: serves reads and writes.
    kSingleton = 0,
    // Values below imply # nodes in chain > 1.
    kHead = 1,
    kMiddle = 2,
    kTail = 3,
  };
  enum class GcsMode : int {
    kNormal = 0,     // (Default) No checkpointing, no flushing.
    kCkptOnly = 1,   // Checkpointing on; flushing off.
    kCkptFlush = 2,  // Both checkpointing & flushing on.
  };
  enum class MasterMode : int {
    kRedis = 0,  // redis-based master.
    kEtcd = 1,   // etcd-based master.
  };

  bool ActAsHead() const {
    return chain_role_ == ChainRole::kSingleton ||
           chain_role_ == ChainRole::kHead;
  }
  bool ActAsTail() const {
    return chain_role_ == ChainRole::kSingleton ||
           chain_role_ == ChainRole::kTail;
  }

  GcsMode gcs_mode() const {
    CHECK(gcs_mode_initialized_);
    return gcs_mode_;
  }
  // Initialized on module startup; immutable afterwards.
  void set_gcs_mode(enum GcsMode mode) {
    CHECK(!gcs_mode_initialized_);
    gcs_mode_ = mode;
    gcs_mode_initialized_ = true;
  }
  std::string gcs_mode_string() const {
    switch (gcs_mode_) {
    case GcsMode::kNormal:
      return "kNormal";
    case GcsMode::kCkptOnly:
      return "kCkptOnly";
    case GcsMode::kCkptFlush:
      return "kCkptFlush";
    default:
      CHECK(false);
    }
  }

  MasterMode master_mode() const {
    CHECK(master_mode_initialized_);
    return master_mode_;
  }
  // Initialized on module startup; immutable afterwards.
  void set_master_mode(enum MasterMode mode) {
    CHECK(!master_mode_initialized_);
    master_mode_ = mode;
    master_mode_initialized_ = true;
  }
  std::string master_mode_string() const {
    switch (master_mode_) {
    case MasterMode::kRedis:
      return "kRedis";
    case MasterMode::kEtcd:
      return "kEtcd";
    default:
      CHECK(false);
    }
  }

  RedisChainModule()
      : chain_role_(ChainRole::kSingleton),
        gcs_mode_(GcsMode::kNormal),
        master_mode_(MasterMode::kRedis),
        parent_(NULL),
        child_(NULL) {
    switch (master_mode_) {
    case MasterMode::kRedis:
      master_client_ = std::unique_ptr<MasterClient>(new RedisMasterClient());
      break;
    case MasterMode::kEtcd:
      CHECK(false) << "Etcd master client is unimplemented";
    default:
      CHECK(false) << "Unrecognized master mode " << master_mode_string();
    }
  }

  ~RedisChainModule() {
    if (child_) redisAsyncFree(child_);
    if (parent_) redisAsyncFree(parent_);
  }

  void Reset(std::string& prev_address,
             std::string& prev_port,
             std::string& next_address,
             std::string& next_port) {
    prev_address_ = prev_address;
    prev_port_ = prev_port;
    next_address_ = next_address;
    next_port_ = next_port;

    // If "c->err" is present, the disconnect callback should've already free'd
    // the async context.
    if (child_ && !child_->err) {
      redisAsyncDisconnect(child_);
    }
    if (parent_ && !parent_->err) {
      redisAsyncDisconnect(parent_);
    }

    child_ = NULL;
    parent_ = NULL;
    if (next_address != "nil") {
      child_ = AsyncConnect(next_address, std::stoi(next_port));
    }
    if (prev_address != "nil") {
      parent_ = AsyncConnect(prev_address, std::stoi(prev_port));
    }
  }

  Status ConnectToMaster(const std::string& address, int port) {
    return master_client_->Connect(address, port);
  }
  MasterClient* Master() { return master_client_.get(); }

  Status OpenCheckpoint(leveldb::DB** db) {
    static leveldb::Options options;
    options.create_if_missing = true;
    return leveldb::DB::Open(options, kCheckpointPath, db);
  }

  void SetRole(ChainRole chain_role) { chain_role_ = chain_role; }
  ChainRole Role() const { return chain_role_; }
  const char* ChainRoleName() const {
    return chain_role_ == ChainRole::kSingleton
               ? "SINGLETON"
               : (chain_role_ == ChainRole::kHead
                      ? "HEAD"
                      : (chain_role_ == ChainRole::kMiddle ? "MIDDLE"
                                                           : "TAIL"));
  }

  std::string prev_address() { return prev_address_; }
  std::string prev_port() { return prev_port_; }
  std::string next_address() { return next_address_; }
  std::string next_port() { return next_port_; }
  redisAsyncContext* child() { return child_; }
  redisAsyncContext* parent() { return parent_; }

  std::set<int64_t>& sent() { return sent_; }

  // Sequence numbers.
  int64_t sn() const { return sn_; }
  int64_t inc_sn() {
    CHECK(ActAsHead())
        << "Logical error?: only the head should increment the sn.";
    // LOG_EVERY_N(INFO, 999999999) << "Using sequence number " << sn_ + 1;
    return ++sn_;
  }
  void record_sn(int64_t sn) { sn_ = std::max(sn_, sn); }

  // A map representing all _in memory_ entries managed by this server.
  std::map<int64_t, std::string>& sn_to_key() { return sn_to_key_; }

  // A map of every in-memory key to the latest seqnum that updates it.
  //
  // Only used for flushing.  Empty if flushing is turned off (kNormal,
  // kCkptOnly).
  std::unordered_map<std::string, int64_t>& key_to_sn() { return key_to_sn_; }

  bool DropWrites() const { return drop_writes_; }
  void SetDropWrites(bool b) { drop_writes_ = b; }

  // Remove from sn_to_key all key s < sn.
  void CleanUpSnToKeyLessThan(int64_t sn);

  // TODO(zongheng): WIP.
  using ChainFunc =
      std::function<int(RedisModuleCtx*, RedisModuleString**, int)>;
  // Runs "node_func" on every node in the chain; after the tail node has run it
  // too, finalizes the mutation by running "tail_func".
  using NodeFunc = std::function<int(
      RedisModuleCtx*, RedisModuleString**, int, RedisModuleString**)>;
  using TailFunc =
      std::function<int(RedisModuleCtx*, RedisModuleString**, int)>;

  // Runs "node_func" on every node in the chain; after the tail node has run it
  // too, finalizes the mutation by running "tail_func".
  int ChainReplicate(RedisModuleCtx* ctx,
                     RedisModuleString** argv,
                     int argc,
                     NodeFunc node_func,
                     TailFunc tail_func);

 private:
  std::string prev_address_;
  std::string prev_port_;
  std::string next_address_;
  std::string next_port_;

  std::unique_ptr<MasterClient> master_client_;

  ChainRole chain_role_;
  enum GcsMode gcs_mode_;
  enum MasterMode master_mode_;
  bool gcs_mode_initialized_ = false;     // To guard against re-initialization.
  bool master_mode_initialized_ = false;  // To guard against re-initialization.

  // The previous node in the chain (or NULL if none)
  redisAsyncContext* parent_;
  // The next node in the chain (or NULL if none)
  redisAsyncContext* child_;

  // Largest sequence number seen so far.  Initialized to the special value -1,
  // which indicates no updates have been processed yet.
  int64_t sn_ = -1;

  // The sent list.
  std::set<int64_t> sent_;

  // A map representing all _in memory_ entries managed by this server.
  //
  // If some seqnum/key pair is flushed out of memory (and flushing algorithm
  // will guarantee it's been checkpointed to disk before), its corresponding
  // entry is deleted from this map.
  //
  // Stored for all GcsMode.
  //
  // NOTE(zongheng): This can be slow.  We should investigate alternatives: (1)
  // 2 vectors, (2) FlatMap.
  std::map<int64_t, std::string> sn_to_key_;

  // A map of every in-memory key to the latest seqnum that updates it.
  //
  // Only used for flushing.  Empty if flushing is turned off (kNormal,
  // kCkptOnly).
  std::unordered_map<std::string, int64_t> key_to_sn_;

  // Drop writes.  Used when adding a child which acts as the new tail.
  bool drop_writes_ = false;

  int MutateHelper(RedisModuleCtx* ctx,
                   RedisModuleString** argv,
                   int argc,
                   NodeFunc node_func,
                   TailFunc tail_func,
                   int sn);
};

int RedisChainModule::MutateHelper(RedisModuleCtx* ctx,
                                   RedisModuleString** argv,
                                   int argc,
                                   NodeFunc node_func,
                                   TailFunc tail_func,
                                   int sn) {
  // Node function.  Retrieve the mutated key.
  RedisModuleString* redis_key_str = nullptr;
  node_func(ctx, argv, argc, &redis_key_str);
  CHECK(redis_key_str != nullptr);

  // State maintenance.
  const std::string key_str = ReadString(redis_key_str);
  DLOG(INFO) << "Mutated key: " << key_str << "; size: " << key_str.size();

  // Update sn_to_key (for all execution modes; used for node addition codepath)
  // and optionally key_to_sn (when flushing is on).
  // NOTE(zongheng): this can be slow, see the note in class declaration.
  sn_to_key()[sn] = key_str;
  if (gcs_mode() == RedisChainModule::GcsMode::kCkptFlush) {
    key_to_sn()[key_str] = sn;
  }
  record_sn(static_cast<int64_t>(sn));

  if (ActAsTail()) {
    tail_func(ctx, argv, argc);
  }
  return REDISMODULE_OK;
}

int RedisChainModule::ChainReplicate(RedisModuleCtx* ctx,
                                     RedisModuleString** argv,
                                     int argc,
                                     NodeFunc node_func,
                                     TailFunc tail_func) {
  CHECK(Role() == RedisChainModule::ChainRole::kSingleton)
      << "ChainReplicate() API supports 1-node mode only for now due to "
         "insufficient "
         "client-side handling";
  if (ActAsHead()) {
    if (!DropWrites()) {
      const long long sn = inc_sn();

      // TODO(zongheng): think about either handling this, or have client supply
      // a seqnum.
      // Return the sequence number.
      // RedisModule_ReplyWithLongLong(ctx, sn);

      return MutateHelper(ctx, argv, argc, node_func, tail_func, sn);
    } else {
      // The store, by contract, is allowed to ignore writes during faults.
      return RedisModule_ReplyWithNull(ctx);
    }
  } else {
    return RedisModule_ReplyWithError(ctx, "ERR called PUT on non-head node");
  }
}

void RedisChainModule::CleanUpSnToKeyLessThan(int64_t sn) {
  auto& map = sn_to_key();

  // We want "iter_upper" to point to sn (or if not exists, the entry
  // immediately after it); so that we implement the delete everything < sn
  // semantics.
  // upper_bound(x) points to the entry after x.
  auto iter_upper = map.upper_bound(sn - 1);
  const int64_t old = map.size();
  // Erase [left, right).
  map.erase(map.begin(), iter_upper);
  const int64_t diff = old - map.size();
  DLOG(INFO) << "Erased from sn_to_key " << diff << " entries; old " << old
             << " new " << map.size();
}

#endif  // CREDIS_CHAIN_MODULE_H_
