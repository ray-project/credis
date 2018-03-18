
#ifndef CREDIS_UTILS_H
#define CREDIS_UTILS_H

#include <vector>
#include "etcd3/include/etcd3.h"

namespace utils {
  class EtcdURL {
   public:
    std::string url;
    std::string address; // host:port
    std::string host;
    int port;
    std::string chain_prefix;
  };
  EtcdURL split_etcd_url(std::string url);
};

#endif //CREDIS_UTILS_H
