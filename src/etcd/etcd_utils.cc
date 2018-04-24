#include <string>

#include "etcd3-cpp/include/etcd3.h"

#include "etcd_utils.h"

EtcdURL SplitEtcdURL(const std::string& url) {
  EtcdURL result;

  auto start_of_chain_prefix = url.find_first_of('/');
  if (start_of_chain_prefix == std::string::npos) {
    result.chain_prefix = "/";
  } else {
    result.chain_prefix = url.substr(start_of_chain_prefix, std::string::npos);
  }
  auto address = url.substr(0, start_of_chain_prefix);
  result.address = address;

  return result;
}
