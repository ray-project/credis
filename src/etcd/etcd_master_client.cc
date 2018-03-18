#include "src/master_client.h"
#include "glog/logging.h"
#include "src/utils.h"
#include "etcd3/include/etcd3.h"
#include "etcd_master_client.h"
#include "etcd_utils.h"

using Watermark = MasterClient::Watermark;

const char* EtcdMasterClient::WatermarkKey(Watermark w) const {
  if (w == MasterClient::Watermark::kSnCkpt) {
    return (url_.chain_prefix + "/_sn_ckpt").c_str();
  } else {
    return (url_.chain_prefix + "/_sn_flushed").c_str();
  }
}

Status EtcdMasterClient::Connect(const std::string& url_str) {
  url_ = utils::split_etcd_url(url_str);
  channel_ = grpc::CreateChannel(url_.address,
                                 grpc::InsecureChannelCredentials());

  // Register heartbeat.
  return Status::OK();
}

Status EtcdMasterClient::GetWatermark(Watermark w, int64_t* val) const {
  auto etcd = etcd3::Client(channel_);
  etcd3::pb::RangeRequest req;
  etcd3::pb::RangeResponse res;
  req.set_key(WatermarkKey(w));
  req.set_range_end("");
  grpc::Status status = etcd.Range(req, &res);

  if (res.kvs_size() > 0) {
    auto kvs = res.kvs();
    const etcd3::pb::KeyValue &result = kvs.Get(0);
    *val = std::stol(result.value());
    return Status::OK();
  } else {
    switch (w) {
      case Watermark::kSnCkpt:
        *val = kSnCkptInit;
        break;
      case Watermark::kSnFlushed:
        *val = kSnFlushedInit;
        break;
    }
    return Status::OK();
  }
}

Status EtcdMasterClient::SetWatermark(Watermark w, int64_t new_val) {
  auto etcd = etcd3::Client(channel_);
  etcd3::pb::PutRequest req;
  etcd3::pb::PutResponse res;
  req.set_key(WatermarkKey(w));
  req.set_value(std::to_string(new_val));
  auto status = etcd.Put(req, &res);
  return Status::OK();
}

Status EtcdMasterClient::Head(std::string* address, int* port) {
  CHECK(false) << "Not implemented";
  return Status::NotSupported("Not implemented");
}

Status EtcdMasterClient::Tail(std::string* address, int* port) {
  CHECK(false) << "Not implemented";
  return Status::NotSupported("Not implemented");
}
