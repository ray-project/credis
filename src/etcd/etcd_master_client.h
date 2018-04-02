#ifndef CREDIS_ETCD_MASTER_CLIENT_H
#define CREDIS_ETCD_MASTER_CLIENT_H

#include "etcd_utils.h"
#include "grpcpp/grpcpp.h"
#include "src/master_client.h"

using Watermark = MasterClient::Watermark;

class EtcdMasterClient : public MasterClient {
 public:
  Status Connect(const std::string& url) override;
  Status Head(std::string* address, int* port) override;
  Status Tail(std::string* address, int* port) override;
  Status GetWatermark(Watermark w, int64_t* val) const override;
  Status SetWatermark(Watermark w, int64_t new_val) override;

 private:
  const char* WatermarkKey(Watermark w) const override;
  std::shared_ptr<grpc::Channel> channel_;
  EtcdURL url_;
};

#endif  // CREDIS_ETCD_MASTER_CLIENT_H
