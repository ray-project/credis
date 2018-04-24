#include "master_client.h"

#include "glog/logging.h"

// #include "utils.h"

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
}

namespace {
redisContext *SyncConnect(const std::string &address, int port) {
  struct timeval timeout = {1, 500000}; // 1.5 seconds
  redisContext *c = redisConnectWithTimeout(address.c_str(), port, timeout);
  if (c == NULL || c->err) {
    if (c) {
      printf("Connection error: %s\n", c->errstr);
      redisFree(c);
    } else {
      printf("Connection error: can't allocate redis context\n");
    }
    std::exit(1);
  }
  return c;
}
} // namespace

Status RedisMasterClient::Connect(const std::string &address, int port) {
  redis_context_.reset(SyncConnect(address, port));
  return Status::OK();
}

const char *MasterClient::WatermarkKey(Watermark w) const {
  return w == MasterClient::Watermark::kSnCkpt ? "_sn_ckpt" : "_sn_flushed";
}

Status RedisMasterClient::GetWatermark(Watermark w, int64_t *val) const {
  redisReply *reply = reinterpret_cast<redisReply *>(
      redisCommand(redis_context_.get(), "GET %s", WatermarkKey(w)));
  const std::string reply_str(reply->str, reply->len); // Can be optimized
  const int reply_type = reply->type;
  freeReplyObject(reply);

  if (reply_type == REDIS_REPLY_NIL) {
    switch (w) {
    case Watermark::kSnCkpt:
      *val = kSnCkptInit;
      break;
    case Watermark::kSnFlushed:
      *val = kSnFlushedInit;
      break;
    default:
      return Status::InvalidArgument("Watermark type incorrect");
    }
    return Status::OK();
  }

  *val = *reinterpret_cast<const int64_t *>(reply_str.data());
  DLOG(INFO) << "GET " << WatermarkKey(w) << ": " << *val;
  return Status::OK();
}

Status RedisMasterClient::SetWatermark(Watermark w, int64_t new_val) {
  const char *new_val_data = reinterpret_cast<const char *>(&new_val);

  redisReply *reply = reinterpret_cast<redisReply *>(
      redisCommand(redis_context_.get(), "SET %s %b", WatermarkKey(w),
                   new_val_data, sizeof(int64_t)));

  std::string reply_str(reply->str, reply->len); // Can be optimized
  DLOG(INFO) << "SET " << WatermarkKey(w) << " " << new_val << ": "
             << reply_str;
  CHECK(reply_str == "OK");

  freeReplyObject(reply);
  return Status::OK();
}

Status RedisMasterClient::Head(std::string *address, int *port) {
  CHECK(false) << "Not implemented";
  return Status::NotSupported("Not implemented");
}
Status RedisMasterClient::Tail(std::string *address, int *port) {
  LOG(INFO) << "Issuing RefreshTail";
  redisReply *reply = reinterpret_cast<redisReply *>(
      redisCommand(redis_context_.get(), "MASTER.REFRESH_TAIL"));

  CHECK(reply != nullptr) << "Error from redisCommand(): code "
                          << redis_context_->err << ", "
                          << std::string(redis_context_->errstr);

  std::string reply_str(reply->str, reply->len);
  LOG(INFO) << "RefreshTail result: " << reply_str;
  freeReplyObject(reply);

  const size_t pos = reply_str.find_first_of(':');
  CHECK(pos != std::string::npos);
  *address = reply_str.substr(0, pos);
  *port = std::atoi(reply_str.substr(pos + 1).data());
  return Status::OK();
}
