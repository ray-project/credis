#ifndef CREDIS_REDIS_MASTER_CLIENT_H
#define CREDIS_REDIS_MASTER_CLIENT_H

#include "master_client.h"

class RedisMasterClient : public MasterClient {
 public:
  virtual Status Connect(const std::string& url) override;
  virtual Status Head(std::string* address, int* port) override;
  virtual Status Tail(std::string* address, int* port) override;
  virtual Status GetWatermark(Watermark w, int64_t* val) const override;
  virtual Status SetWatermark(Watermark w, int64_t new_val) override;

 private:
  virtual const char* WatermarkKey(Watermark w) const override;
  std::unique_ptr<redisContext> redis_context_;
};

#endif  // CREDIS_REDIS_MASTER_CLIENT_H
