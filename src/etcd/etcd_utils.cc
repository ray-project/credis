
#include "etcd_utils.h"
#include <string>
#include "etcd3/include/etcd3.h"

EtcdURL SplitEtcdURL(const std::string& url) {
  EtcdURL result;
  result.url = url;

  auto start_of_chain_prefix = url.find_first_of('/');
  if (start_of_chain_prefix == std::string::npos) {
    result.chain_prefix = "/";
  } else {
    result.chain_prefix = url.substr(start_of_chain_prefix, std::string::npos);
  }
  auto address = url.substr(0, start_of_chain_prefix);
  result.address = address;

  auto start_of_port = address.find_first_of(':') + 1;
  auto host = address.substr(0, start_of_port - 1);
  auto port_str = address.substr(start_of_port);
  result.host = host;
  result.port = std::stoi(port_str);
  return result;
}
