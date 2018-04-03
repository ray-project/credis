#ifndef CREDIS_MASTER_CLIENT_H_
#define CREDIS_MASTER_CLIENT_H_

// A client for all chain nodes to talk to the master.
//
// The default implementation assumes a redis-based master.  It is possible that
// in the future, this interface can be backed by other implementation, such as
// etcd or consul.

#include <memory>
#include <string>

extern "C" {
#include "hiredis/hiredis.h"
}

#include "leveldb/db.h"  // For leveldb::Status.

using Status = leveldb::Status;

class MasterClient {
 public:
  enum class Watermark : int {
    kSnCkpt = 0,
    kSnFlushed = 1,
  };

  virtual Status Connect(const std::string& address, int port) = 0;

  // TODO(zongheng): impl.
  // Retries the current head and tail nodes (for writes and reads,
  // respectively).
  virtual Status Head(std::string* address, int* port) = 0;
  virtual Status Tail(std::string* address, int* port) = 0;

  // Watermark sequence numbers
  //
  // The master manages and acts as the source-of-truth for watermarks.
  //
  // Definitions:
  //   sn_ckpt: next/smallest sn yet to be checkpointed;
  //            thus, [0, sn_ckpt) is the currently checkpointed range.
  //   sn_flushed: next/smallest sn yet to be flushed;
  //            thus, [0, sn_flushed) is the currently flushed range.
  //
  // Properties of various watermarks (and their extreme cases):
  //   sn_ckpt <= sn_latest_tail + 1 (i.e., everything has been checkpointed)
  //   sn_flushed <= sn_ckpt (i.e., all checkpointed data has been flushed)
  virtual Status GetWatermark(Watermark w, int64_t* val) const = 0;
  virtual Status SetWatermark(Watermark w, int64_t new_val) = 0;

 protected:
  const char* WatermarkKey(Watermark w) const;

  static constexpr int64_t kSnCkptInit = 0;
  static constexpr int64_t kSnFlushedInit = 0;
};

class RedisMasterClient : public MasterClient {
 public:
  Status Connect(const std::string& address, int port) override;
  Status Head(std::string* address, int* port) override;
  Status Tail(std::string* address, int* port) override;
  Status GetWatermark(Watermark w, int64_t* val) const override;
  Status SetWatermark(Watermark w, int64_t new_val) override;

 private:
  std::unique_ptr<redisContext> redis_context_;
};

#endif  // CREDIS_MASTER_CLIENT_H_
