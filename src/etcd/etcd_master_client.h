#ifndef CREDIS_ETCD_MASTER_CLIENT_H
#define CREDIS_ETCD_MASTER_CLIENT_H

#include "grpcpp/grpcpp.h"
#include "src/master_client.h"
#include "etcd_utils.h"

using Watermark = MasterClient::Watermark;

class EtcdMasterClient : public MasterClient {
 public:
  virtual Status Connect(const std::string& url) override;
  virtual Status Head(std::string* address, int* port) override;
  virtual Status Tail(std::string* address, int* port) override;
  virtual Status GetWatermark(Watermark w, int64_t* val) const override;
  virtual Status SetWatermark(Watermark w, int64_t new_val) override;

 private:
  virtual const char* WatermarkKey(Watermark w) const override;
  std::shared_ptr<grpc::Channel> channel_;
  utils::EtcdURL url_;
};

#endif //CREDIS_ETCD_MASTER_CLIENT_H
