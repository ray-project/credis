
#ifndef CREDIS_UTILS_H
#define CREDIS_UTILS_H

#include <vector>
#include "etcd3/include/etcd3.h"

// Represents an etcd URL (e.g. "127.0.0.1:1234/prefix").
struct EtcdURL {
 public:
  // the full url string (127.0.0.1:1234/prefix)
  std::string url;
  // 127.0.0.1:1234
  std::string address;
  // 127.0.0.1
  std::string host;
  // 1234
  int port;
  // "/prefix"
  std::string chain_prefix;
};
EtcdURL SplitEtcdURL(const std::string& url);

#endif  // CREDIS_UTILS_H
