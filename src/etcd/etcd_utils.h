#ifndef CREDIS_ETCD_UTILS_H
#define CREDIS_ETCD_UTILS_H

#include <vector>

#include "etcd3-cpp/include/etcd3.h"

// Represents an etcd URL (e.g. "127.0.0.1:1234/prefix").
struct EtcdURL {
 public:
  // 127.0.0.1:1234
  std::string address;
  // "/prefix"
  std::string chain_prefix;
};
EtcdURL SplitEtcdURL(const std::string& url);

#endif  // CREDIS_ETCD_UTILS_H
