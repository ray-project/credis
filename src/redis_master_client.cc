#include "redis_master_client.h"
#include "glog/logging.h"
#include "utils.h"

const char* RedisMasterClient::WatermarkKey(Watermark w) const {
  return w == MasterClient::Watermark::kSnCkpt ? "_sn_ckpt" : "_sn_flushed";
}

Status RedisMasterClient::Connect(const std::string& url) {
  auto address = url.substr(0, url.find_first_of(":"));
  int port = std::stoi(url.substr(url.find_first_of(":") + 1));
  redis_context_.reset(SyncConnect(address, port));
  return Status::OK();
}

Status RedisMasterClient::GetWatermark(Watermark w, int64_t* val) const {
  redisReply* reply = reinterpret_cast<redisReply*>(
      redisCommand(redis_context_.get(), "GET %s", WatermarkKey(w)));
  const std::string reply_str(reply->str, reply->len);  // Can be optimized
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

  *val = *reinterpret_cast<const int64_t*>(reply_str.data());
  DLOG(INFO) << "GET " << WatermarkKey(w) << ": " << *val;
  return Status::OK();
}

Status RedisMasterClient::SetWatermark(Watermark w, int64_t new_val) {
  const char* new_val_data = reinterpret_cast<const char*>(&new_val);

  redisReply* reply = reinterpret_cast<redisReply*>(
      redisCommand(redis_context_.get(), "SET %s %b", WatermarkKey(w),
                   new_val_data, sizeof(int64_t)));

  std::string reply_str(reply->str, reply->len);  // Can be optimized
  DLOG(INFO) << "SET " << WatermarkKey(w) << " " << new_val << ": "
             << reply_str;
  CHECK(reply_str == "OK");

  freeReplyObject(reply);
  return Status::OK();
}

Status RedisMasterClient::Head(std::string* address, int* port) {
  CHECK(false) << "Not implemented";
  return Status::NotSupported("Not implemented");
}
Status RedisMasterClient::Tail(std::string* address, int* port) {
  CHECK(false) << "Not implemented";
  return Status::NotSupported("Not implemented");
}
