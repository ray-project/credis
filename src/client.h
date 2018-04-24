#ifndef CREDIS_CLIENT_H_
#define CREDIS_CLIENT_H_

#include <stdlib.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

extern "C" {
#include "hiredis/adapters/ae.h"
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
}

#include "glog/logging.h"
#include "leveldb/db.h"

using Status = leveldb::Status;

class RedisCallbackManager {
public:
  using RedisCallback = std::function<void(const std::string &)>;

  static RedisCallbackManager &instance() {
    static RedisCallbackManager instance;
    return instance;
  }

  int64_t add(const RedisCallback &function);

  RedisCallback &get(int64_t callback_index);

private:
  RedisCallbackManager() : num_callbacks(0){};

  ~RedisCallbackManager() {}

  int64_t num_callbacks;
  std::unordered_map<int64_t, std::unique_ptr<RedisCallback>> callbacks_;
};

class RedisClient {
public:
  RedisClient() {}
  ~RedisClient();
  // TODO: this should really be (addr, port) pairs.
  // Allows using different ports for write and ack.
  Status Connect(const std::string &address, int write_port, int ack_port);
  // Use the same port for both write and ack.
  Status Connect(const std::string &address, int port);
  Status AttachToEventLoop(aeEventLoop *loop);
  Status RegisterAckCallback(redisCallbackFn *callback);
  Status RunAsync(const std::string &command, const std::string &id,
                  const char *data, size_t length, int64_t callback_index);

  Status ReconnectAckContext(const std::string &address, int port,
                             redisCallbackFn *callback);

  // Does not transfer ownership.
  redisContext *context() const { return context_; };
  redisAsyncContext *write_context() const { return write_context_; };
  redisAsyncContext *read_context() const { return read_context_; };

private:
  // Cache user-supplied loop in case we need to attach new/reconnected contexts
  // to the loop.  Not owned.
  aeEventLoop *loop_;

  redisContext *context_;
  redisAsyncContext *write_context_;
  redisAsyncContext *read_context_;
  redisAsyncContext *ack_subscribe_context_;
};

#endif // CREDIS_CLIENT_H_
