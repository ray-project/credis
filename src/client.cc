#include "client.h"

// This is a global redis callback which will be registered for every
// asynchronous redis call. It dispatches the appropriate callback
// that was registered with the RedisCallbackManager.
void GlobalRedisCallback(void* c, void* r, void* privdata) {
  if (r == NULL) {
    return;
  }
  int64_t callback_index = reinterpret_cast<int64_t>(privdata);
  redisReply* reply = reinterpret_cast<redisReply*>(r);
  std::string data = "";
  if (reply->type == REDIS_REPLY_NIL) {
  } else if (reply->type == REDIS_REPLY_STRING) {
    data = std::string(reply->str, reply->len);
  } else if (reply->type == REDIS_REPLY_INTEGER) {
    data = std::to_string(reply->integer);
  } else if (reply->type == REDIS_REPLY_STATUS) {
  } else if (reply->type == REDIS_REPLY_ERROR) {
    LOG(ERROR) << "Redis error " << reply->str;
  } else {
    LOG(ERROR) << "Fatal redis error of type " << reply->type
               << " and with string " << std::endl;
  }
  RedisCallbackManager::instance().get(callback_index)(data);
}

int64_t RedisCallbackManager::add(const RedisCallback& function) {
  callbacks_.emplace(num_callbacks, std::unique_ptr<RedisCallback>(
                                        new RedisCallback(function)));
  return num_callbacks++;
}

RedisCallbackManager::RedisCallback& RedisCallbackManager::get(
    int64_t callback_index) {
  return *callbacks_[callback_index];
}

#define REDIS_CHECK_ERROR(CONTEXT, REPLY)                     \
  if (REPLY == nullptr || REPLY->type == REDIS_REPLY_ERROR) { \
    return Status::IOError(CONTEXT->errstr);                  \
  }

RedisClient::~RedisClient() {
  if (context_) redisFree(context_);
  if (write_context_) redisAsyncFree(write_context_);
  if (read_context_) redisAsyncFree(read_context_);
}

constexpr int64_t kRedisDBConnectRetries = 50;
constexpr int64_t kRedisDBWaitMilliseconds = 100;

namespace {
Status ConnectContext(const std::string& address,
                      int port,
                      redisAsyncContext** context) {
  redisAsyncContext* ctx = redisAsyncConnect(address.c_str(), port);
  if (ctx == nullptr || ctx->err) {
    LOG(ERROR) << "Could not establish connection to redis " << address << ":"
               << port;
    return Status::IOError("ERR");
  }
  *context = ctx;
  return Status::OK();
}
}  // namespace

Status RedisClient::Connect(const std::string& address,
                            int write_port,
                            int ack_port) {
  int connection_attempts = 0;
  context_ = redisConnect(address.c_str(), write_port);
  while (context_ == nullptr || context_->err) {
    if (connection_attempts >= kRedisDBConnectRetries) {
      if (context_ == nullptr) {
        LOG(ERROR) << "Could not allocate redis context.";
      }
      if (context_->err) {
        LOG(ERROR) << "Could not establish connection to redis " << address
                   << ":" << write_port;
      }
      break;
    }
    LOG(ERROR) << "Failed to connect to Redis, retrying.";
    // Sleep for a little.
    usleep(kRedisDBWaitMilliseconds * 1000);
    context_ = redisConnect(address.c_str(), write_port);
    connection_attempts += 1;
  }
  redisReply* reply = reinterpret_cast<redisReply*>(
      redisCommand(context_, "CONFIG SET notify-keyspace-events Kl"));
  REDIS_CHECK_ERROR(context_, reply);

  // Connect to async contexts.
  CHECK(ConnectContext(address, write_port, &write_context_).ok());
  CHECK(ConnectContext(address, ack_port, &read_context_).ok());
  CHECK(ConnectContext(address, ack_port, &ack_subscribe_context_).ok());
  return Status::OK();
}

Status RedisClient::Connect(const std::string& address, int port) {
  return Connect(address, port, port);
}

Status RedisClient::AttachToEventLoop(aeEventLoop* loop) {
  if (redisAeAttach(loop, write_context_) != REDIS_OK) {
    return Status::IOError("could not attach redis event loop");
  }
  if (redisAeAttach(loop, read_context_) != REDIS_OK) {
    return Status::IOError("could not attach redis event loop");
  }
  if (redisAeAttach(loop, ack_subscribe_context_) != REDIS_OK) {
    return Status::IOError("could not attach redis event loop");
  }
  return Status::OK();
}
// reinterpret_cast<redisCallbackFn *>
Status RedisClient::RegisterAckCallback(redisCallbackFn* callback) {
  // static const char* kChan = "answers";
  const std::string kChan = std::to_string(getpid());
  LOG(INFO) << getpid() << " subscribing to chan " << kChan;
  const int status = redisAsyncCommand(ack_subscribe_context_, callback,
                                       /*privdata=*/NULL, "SUBSCRIBE %b",
                                       kChan.c_str(), kChan.size());
  if (status == REDIS_ERR) {
    return Status::IOError(std::string(ack_subscribe_context_->errstr));
  }
  return Status::OK();
}

Status RedisClient::RunAsync(const std::string& command,
                             const std::string& id,
                             const char* data,
                             size_t length,
                             int64_t callback_index) {
  if (length > 0) {
    std::string redis_command = command + " %b %b";
    int status = redisAsyncCommand(
        write_context_,
        reinterpret_cast<redisCallbackFn*>(&GlobalRedisCallback),
        reinterpret_cast<void*>(callback_index), redis_command.c_str(),
        id.data(), id.size(), data, length);
    if (status == REDIS_ERR) {
      return Status::IOError(std::string(write_context_->errstr));
    }
  } else {
    std::string redis_command = command + " %b";
    int status = redisAsyncCommand(
        write_context_,
        reinterpret_cast<redisCallbackFn*>(&GlobalRedisCallback),
        reinterpret_cast<void*>(callback_index), redis_command.c_str(),
        id.data(), id.size());
    if (status == REDIS_ERR) {
      return Status::IOError(std::string(write_context_->errstr));
    }
  }
  return Status::OK();
}
