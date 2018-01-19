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

#include "leveldb/db.h"

using Status = leveldb::Status;

class MasterClient {
 public:
  enum class Watermark : int {
    kSnCkpt = 0,
    kSnFlushed = 1,
  };

  Status Connect(const std::string& address, int port);

  // TODO(zongheng): impl.
  // Retries the current head and tail nodes (for writes and reads,
  // respectively).
  Status Head(std::string* address, int* port);
  Status Tail(std::string* address, int* port);

  // Watermark sequence numbers
  //
  // The master manages and acts as the source-of-truth for watermarks.
  //
  // Properties of various watermarks (and their extreme cases):
  //   sn_ckpt <= sn_latest_tail + 1 (i.e., everything has been checkpointed)
  //   sn_flushed < sn_ckpt (i.e., all checkpointed data has been flushed)

  Status GetWatermark(Watermark w, int64_t* val) const;
  Status SetWatermark(Watermark w, int64_t new_val);

 private:
  const char* WatermarkKey(Watermark w) const;

  std::unique_ptr<redisContext> redis_context_;

  static constexpr int64_t kSnCkptInit = 0;
  static constexpr int64_t kSnFlushedInit = 0;
};

#endif  // CREDIS_MASTER_CLIENT_H_
