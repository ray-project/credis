#include <assert.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <map>
#include <set>
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

#include "etcd/etcd_master_client.h"
#include "glog/logging.h"
#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "master_client.h"
#include "redis_master_client.h"
#include "timer.h"
#include "utils.h"

Timer timer;

const char* const kCheckpointPath =
    "/tmp/gcs_ckpt";  // TODO(zongheng): don't hardcode.
const char* const kCheckpointHeaderKey = "";
const char* const kStringZero = "0";
const char* const kStringOne = "1";

extern "C" {
aeEventLoop* getEventLoop();
int getPort();
char* getBindAddr();
}

using Clock = std::chrono::steady_clock;
using Ms = std::chrono::milliseconds;
using TimePoint = std::chrono::time_point<Clock, Ms>;

long long int heartbeatTimeoutSec = 15;
long long int heartbeatIntervalSec = 3;

struct HeartbeatInfo {
  int64_t lease_id;
  EtcdURL etcd_url;
  std::shared_ptr<grpc::Channel> channel;
  std::chrono::time_point<Clock, Ms> last_beat;
} hb_info;

// This is an instance of aeTimeProc.
int heartbeat(aeEventLoop* loop, long long id, void* hb_info_ptr) {
  auto current_hb = ((HeartbeatInfo*) hb_info_ptr);

  // Ensure that at least 1/2 of the heartbeat interval has really elapsed
  // since we sent an RPC to etcd. Redis will call this function repeatedly
  // really fast if it has nothing else to do, DoSing etcd in the process.
  auto now = std::chrono::time_point_cast<Ms>(Clock::now());
  auto half_hb = std::chrono::milliseconds{(heartbeatIntervalSec / 2) * 1000};

  if ((now - current_hb->last_beat) < half_hb) {
    return 0;
  }
  current_hb->last_beat = now;

  etcd3::Client etcd(current_hb->channel);
  etcd3::pb::LeaseKeepAliveRequest req;
  req.set_id(current_hb->lease_id);
  etcd3::pb::LeaseKeepAliveResponse res;
  auto status = etcd.LeaseKeepAlive(req, &res);
  CHECK(status.ok()) << "Failed to KeepAlive lease " << current_hb->lease_id
                     << ". "
                     << "GRPC error " << status.error_code() << ": "
                     << status.error_message();

  return 0;
}

using Status = leveldb::Status;  // So that it can be easily replaced.

// TODO(zongheng): whenever we are about to do redisAsyncCommand(remote, ...),
// we should test for remote->err first.  See the NOTE in Put().

namespace {

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

  GcsMode GetGcsMode() const {
    CHECK(gcs_mode_initialized_);
    return gcs_mode_;
  }
  // Initialized on module startup; immutable afterwards.
  void SetGcsMode(enum GcsMode mode) {
    CHECK(!gcs_mode_initialized_);
    gcs_mode_ = mode;
    gcs_mode_initialized_ = true;
  }
  std::string GcsModeString() const {
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

  MasterMode GetMasterMode() const {
    CHECK(master_mode_initialized_);
    return master_mode_;
  }
  // Initialized on module startup; immutable afterwards.
  void SetMasterMode(enum MasterMode mode) {
    CHECK(!master_mode_initialized_);
    master_mode_ = mode;
    master_mode_initialized_ = true;
  }
  std::string MasterModeString() const {
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
      master_client_ = std::unique_ptr<MasterClient>(new EtcdMasterClient());
    default:
      CHECK(false) << "Unrecognized master mode " << MasterModeString();
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

  Status ConnectToMaster(const std::string& url) {
    return master_client_->Connect(url);
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

  // Start heartbeat in etcd.
  grpc::Status StartHeartbeat(EtcdURL url,
                              std::string own_addr,
                              int own_port,
                              aeEventLoop* el) {
    auto channel =
        grpc::CreateChannel(url.address, grpc::InsecureChannelCredentials());
    etcd3::Client etcd(channel);

    // Establish heartbeat.
    int64_t lease_id;
    {
      etcd3::pb::LeaseGrantRequest req;
      req.set_ttl(heartbeatTimeoutSec);
      etcd3::pb::LeaseGrantResponse res;
      auto status = etcd.LeaseGrant(req, &res);
      if (!status.ok()) {
        return status;
      }
      lease_id = res.id();
    }

    hb_info.lease_id = lease_id;
    hb_info.etcd_url = url;
    hb_info.channel = channel;
    hb_info.last_beat = std::chrono::time_point_cast<Ms>(Clock::now());
    // Schedule a heartbeat every 10 seconds.
    aeCreateTimeEvent(el, 1000, &heartbeat, &hb_info, NULL);

    {
      etcd3::pb::PutRequest req;
      auto own_addr_port = own_addr + ":" + std::to_string(own_port);
      req.set_key(url.chain_prefix + "/heartbeat/" + own_addr_port);
      req.set_value("unused");
      req.set_lease(lease_id);
      etcd3::pb::PutResponse res;
      auto status = etcd.Put(req, &res);
      if (!status.ok()) {
        return status;
      }
    }

    return grpc::Status::OK;
  }

 private:
  std::string prev_address_;
  std::string prev_port_;
  std::string next_address_;
  std::string next_port_;

  std::unique_ptr<MasterClient> master_client_;
  std::string master_url_;

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
};

static RedisChainModule module;

void RedisChainModule::CleanUpSnToKeyLessThan(int64_t sn) {
  auto& map = module.sn_to_key();

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

int DoFlush(RedisModuleCtx* ctx,
            int64_t sn_left,
            int64_t sn_right,
            int64_t sn_ckpt,
            const std::vector<std::string>& flushable_keys) {
  CHECK(module.GetGcsMode() == RedisChainModule::GcsMode::kCkptFlush);

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
void CollectFlushableKeys(int64_t sn_left,
                          int64_t sn_right,
                          int64_t sn_ckpt,
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
int Put(RedisModuleCtx* ctx,
        RedisModuleString* name,
        RedisModuleString* data,
        RedisModuleString* client_id,
        long long sn,
        bool is_flush) {
  // State maintenance.
  if (is_flush) {
    RedisModuleKey* key = reinterpret_cast<RedisModuleKey*>(
        RedisModule_OpenKey(ctx, name, REDISMODULE_WRITE));
    RedisModule_DeleteKey(key);
    module.sn_to_key().erase(sn);
    // The tail has the responsibility of updating the sn_flushed watermark.
    if (module.ActAsTail()) {
      // "sn + 1" is the next sn to be flushed.
      module.Master()->SetWatermark(MasterClient::Watermark::kSnFlushed,
                                    sn + 1);
    }
    RedisModule_CloseKey(key);
  } else {
    RedisModuleKey* key = reinterpret_cast<RedisModuleKey*>(
        RedisModule_OpenKey(ctx, name, REDISMODULE_WRITE));
    CHECK(REDISMODULE_OK == RedisModule_StringSet(key, data))
        << "key " << key << " sn " << sn;
    RedisModule_CloseKey(key);

    // Update sn_to_key (for all execution modes; used for node addition
    // codepath) and optionally key_to_sn (when flushing is on).
    const std::string key_str(ReadString(name));
    // NOTE(zongheng): this can be slow, see the note in class declaration.
    module.sn_to_key()[sn] = key_str;
    if (module.GetGcsMode() == RedisChainModule::GcsMode::kCkptFlush) {
      module.key_to_sn()[key_str] = sn;
    }
    module.record_sn(static_cast<int64_t>(sn));
  }

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

    if (module.parent()) {
      RedisModuleCallReply* reply =
          RedisModule_Call(ctx, "MEMBER.ACK", "c", seqnum_str.c_str());
      if (RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
        return RedisModule_ReplyWithCallReply(ctx, reply);
      }
    }
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
          module.child(), NULL, NULL, "MEMBER.PROPAGATE %b %b %b %s %b",
          key_ptr, key_len, val_ptr, val_len, seqnum_str.data(),
          seqnum_str.size(), is_flush ? kStringOne : kStringZero, cid_ptr,
          cid_len);
      // TODO(zongheng): check status.
      // LOG_EVERY_N(INFO, 999999999) << "Done";
      module.sent().insert(sn);
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

// Resend all unacked updates.
int MemberResendUnacked_RedisCommand(RedisModuleCtx* ctx,
                                     RedisModuleString** argv,
                                     int argc) {
  if (argc != 1) return RedisModule_WrongArity(ctx);
  auto& sn_to_key = module.sn_to_key();
  for (auto sn : module.sent()) {
    auto sn_string = std::to_string(sn);
    std::string key = sn_to_key[sn];
    KeyReader reader(ctx, key);
    size_t size;
    const char* value = reader.value(&size);
    if (!module.child()->err) {
      const int status = redisAsyncCommand(
          module.child(), NULL, NULL, "MEMBER.PROPAGATE %b %b %b %s",
          key.data(), key.size(), value, size, sn_string.data(),
          sn_string.size(), /*is_flush=*/kStringZero);
      // TODO(zongheng): check status.
    }
  }
  return RedisModule_ReplyWithNull(ctx);
}

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
int MemberSetRole_RedisCommand(RedisModuleCtx* ctx,
                               RedisModuleString** argv,
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
            module.child(), NULL, NULL, "MEMBER.PROPAGATE %b %b %b %s",
            key.data(), key.size(), value, size, sn.data(), sn.size(),
            /*is_flush=*/kStringZero);
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

// Arguments:
//  argv[1] = master URL (string): e.g. redis_host:port or etcd_host:port/prefix
//  argv[2] = (OPTIONAL, etcd only) time between heartbeats in seconds
//  argv[3] = (OPTIONAL, etcd only) time till heartbeat expires in seconds
int MemberConnectToMaster_RedisCommand(RedisModuleCtx* ctx,
                                       RedisModuleString** argv,
                                       int argc) {
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }

  std::string master_url = ReadString(argv[1]);

  if (module.GetMasterMode() == RedisChainModule::MasterMode::kEtcd) {
    // Optional arguments for setting the heartbeat interval and timeout.
    if (argc >= 4) {
      RedisModule_StringToLongLong(argv[2], &heartbeatIntervalSec);
      RedisModule_StringToLongLong(argv[3], &heartbeatTimeoutSec);
    }

    Status s = module.ConnectToMaster(master_url);

    auto url = SplitEtcdURL(master_url);
    std::string own_addr = "127.0.0.1";
    char* bind_addr = getBindAddr();
    if (bind_addr != NULL) {
      own_addr = bind_addr;
    }
    module.StartHeartbeat(url, own_addr, getPort(), getEventLoop());
    if (!s.ok()) return RedisModule_ReplyWithError(ctx, s.ToString().data());
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }

  // TODO gchao: There is some code duplication here but my refactorings keep
  // turning out hard to read. DRY it out somehow.
  Status s = module.ConnectToMaster(master_url);
  if (!s.ok()) return RedisModule_ReplyWithError(ctx, s.ToString().data());
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

// Put a key. This is only called on the head node by the client.
// argv[1] is the key for the data
// argv[2] is the data
// argv[3] is unique client id
int MemberPut_RedisCommand(RedisModuleCtx* ctx,
                           RedisModuleString** argv,
                           int argc) {
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  // LOG(INFO) << "MemberPut";
  if (module.ActAsHead()) {
    // LOG(INFO) << "MemberPut";
    if (!module.DropWrites()) {
      const long long sn = module.inc_sn();
      // Return the sequence number
      RedisModule_ReplyWithLongLong(ctx, sn);

      // TODO(zongheng): which one is faster?
      // const std::string sn_string = std::to_string(sn);
      // RedisModule_ReplyWithStringBuffer(ctx, sn_string.data(),
      //                                  sn_string.size());

      // LOG(INFO) << "MemberPut, assigning new sn " << sn;
      return Put(ctx, argv[1], argv[2], argv[3], sn,
                 /*is_flush=*/false);
    } else {
      // The store, by contract, is allowed to ignore writes during faults.
      return RedisModule_ReplyWithNull(ctx);
      // return RedisModule_ReplyWithError(
      //     ctx, "Server set to drop_writes mode, retry?");
    }
  } else {
    return RedisModule_ReplyWithError(ctx, "ERR called PUT on non-head node");
  }
}

// Propagate a put request down the chain
// argv[1] is the key for the data
// argv[2] is the data
// argv[3] is the sequence number for this update request
// argv[4] is a long long, either 0 or 1, indicating "is_flush".
// argv[5] is unique client id
int MemberPropagate_RedisCommand(RedisModuleCtx* ctx,
                                 RedisModuleString** argv,
                                 int argc) {
  if (argc != 6) {
    return RedisModule_WrongArity(ctx);
  }
  if (!module.DropWrites()) {
    long long sn = -1, is_flush = 0;
    RedisModule_StringToLongLong(argv[3], &sn);
    RedisModule_StringToLongLong(argv[4], &is_flush);
    return Put(ctx, argv[1], argv[2], argv[5], sn,
               is_flush == 0 ? false : true);
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
int MemberNoPropPut_RedisCommand(RedisModuleCtx* ctx,
                                 RedisModuleString** argv,
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

// Replicate our content to our child.  This command is used when a node is
// being added as the new tail of the whole chain.

// NOTE: with Sent_T handling this can be asynchronous (in the tail-add path);
// however without handling Sent_T this function should probably be
// synchronous.
int MemberReplicate_RedisCommand(RedisModuleCtx* ctx,
                                 RedisModuleString** argv,
                                 int argc) {
  REDISMODULE_NOT_USED(argv);
  if (argc != 1) {
    return RedisModule_WrongArity(ctx);
  }

  if (module.child()) {
    DLOG(INFO) << "Called replicate. sn_to_key size "
               << module.sn_to_key().size();
    // NOTE(zongheng): we basically use "sn_to_key" to iterate through in-memory
    // state for convenience only.  Presumably, we can rely on some other
    // mechanisms, such as redis' native iterator, to do this.
    for (auto element : module.sn_to_key()) {
      KeyReader reader(ctx, element.second);
      size_t key_size, value_size;
      const char* key_data = reader.key(&key_size);
      const char* value_data = reader.value(&value_size);
      if (!module.child()->err) {
        const std::string sn = std::to_string(element.first);
        const int status = redisAsyncCommand(
            module.child(), NULL, NULL, "MEMBER.NO_PROP_PUT %b %b %b", key_data,
            key_size, value_data, value_size, sn.data(), sn.size());
        // TODO(zongheng): check status.
      } else {
        // LOG_EVERY_N(INFO, 999999999)
        //     << "Child dead, waiting for master to intervene.";
        // LOG_EVERY_N(INFO, 999999999)
        //     << "Redis context error: '" <<
        //     std::string(module.child()->errstr)
        //     << "'.";
      }
    }
  }
  return RedisModule_ReplyWithNull(ctx);
}

// Send ack up the chain.
// This could be batched in the future if we want to.
// argv[1] is the sequence number that is acknowledged
int MemberAck_RedisCommand(RedisModuleCtx* ctx,
                           RedisModuleString** argv,
                           int argc) {
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  std::string sn = ReadString(argv[1]);
  // LOG_EVERY_N(INFO, 999999999)
  //     << "Erasing sequence number " << sn << " from sent list";
  module.sent().erase(std::stoi(sn));
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
                                     RedisModuleString** argv,
                                     int argc) {
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
int TailCheckpoint_RedisCommand(RedisModuleCtx* ctx,
                                RedisModuleString** argv,
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
  if (module.GetGcsMode() == RedisChainModule::GcsMode::kNormal) {
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
int HeadFlush_RedisCommand(RedisModuleCtx* ctx,
                           RedisModuleString** argv,
                           int argc) {
  const double start = timer.NowMicrosecs();
  REDISMODULE_NOT_USED(argv);
  if (argc != 1) return RedisModule_WrongArity(ctx);  // No args needed.
  if (!module.ActAsHead()) {
    return RedisModule_ReplyWithError(
        ctx, "ERR this command must be called on the head.");
  }
  if (module.GetGcsMode() != RedisChainModule::GcsMode::kCkptFlush) {
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
int MemberFlush_RedisCommand(RedisModuleCtx* ctx,
                             RedisModuleString** argv,
                             int argc) {
  if (argc != 4) return RedisModule_WrongArity(ctx);

  long long sn_left, sn_right, sn_ckpt;
  RedisModule_StringToLongLong(argv[1], &sn_left);
  RedisModule_StringToLongLong(argv[2], &sn_right);
  RedisModule_StringToLongLong(argv[3], &sn_ckpt);

  std::vector<std::string> flushable_keys;
  CollectFlushableKeys(sn_left, sn_right, sn_ckpt, &flushable_keys);
  DoFlush(ctx, sn_left, sn_right, sn_ckpt, flushable_keys);

  // TODO(zongheng): is this needed?  Do we want ACKs flying in-between servers?
  return RedisModule_ReplyWithNull(ctx);
}

// LIST.CHECKPOINT: print to stdout all keys, values in the checkpoint file.
//
// For debugging.
int ListCheckpoint_RedisCommand(RedisModuleCtx* ctx,
                                RedisModuleString** argv,
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
  } else {
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
  }
}

// MEMBER.SN: the largest SN processed by this node.
//
// For debugging.
int MemberSn_RedisCommand(RedisModuleCtx* ctx,
                          RedisModuleString** argv,
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
int RedisModule_OnLoad(RedisModuleCtx* ctx,
                       RedisModuleString** argv,
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
    module.SetGcsMode(RedisChainModule::GcsMode::kNormal);
    break;
  case 1:
    module.SetGcsMode(RedisChainModule::GcsMode::kCkptOnly);
    break;
  case 2:
    module.SetGcsMode(RedisChainModule::GcsMode::kCkptFlush);
    break;
  default:
    return REDISMODULE_ERR;
  }
  RedisModule_Log(ctx, "notice", "GCS mode: %s",
                  module.GcsModeString().c_str());

  long long master_mode = 0;
  if (argc > 1) {
    CHECK_EQ(REDISMODULE_OK,
             RedisModule_StringToLongLong(argv[1], &master_mode));
  }
  switch (master_mode) {
  case 0:
    module.SetMasterMode(RedisChainModule::MasterMode::kRedis);
    break;
  case 1:
    module.SetMasterMode(RedisChainModule::MasterMode::kEtcd);
    break;
  default:
    return REDISMODULE_ERR;
  }
  RedisModule_Log(ctx, "notice", "Master mode: %s",
                  module.MasterModeString().c_str());

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

  if (RedisModule_CreateCommand(ctx, "MEMBER.RESEND_UNACKED",
                                MemberResendUnacked_RedisCommand, "write",
                                /*firstkey=*/-1, /*lastkey=*/-1,
                                /*keystep=*/0) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}
}
