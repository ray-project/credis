#include "utils.h"

#include "cstdlib"

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
}

const char* ReadString(const RedisModuleString* str, size_t* len) {
  return RedisModule_StringPtrLen(str, len);
}

std::string ReadString(RedisModuleString* str) {
  size_t l = 0;
  const char* s = ReadString(str, &l);
  return std::string(s, l);
}

bool AuthenticateContext(redisContext* c, const std::string& password) {
  redisReply* reply = nullptr;
  if (password == "") {
    reply = reinterpret_cast<redisReply*>(redisCommand(c, "PING"));
  } else {
    reply = reinterpret_cast<redisReply*>(
        redisCommand(c, "AUTH %s", password.c_str()));
  }

  bool authenticated = (reply != nullptr && reply->type != REDIS_REPLY_ERROR);
  freeReplyObject(reply);
  return authenticated;
}

redisContext* SyncConnect(const std::string& address, int port,
                          const std::string& password) {
  struct timeval timeout = {1, 500000};  // 1.5 seconds
  redisContext* c = redisConnectWithTimeout(address.c_str(), port, timeout);
  if (c == NULL || c->err) {
    if (c) {
      printf("Connection error: %s\n", c->errstr);
      redisFree(c);
    } else {
      printf("Connection error: can't allocate redis context\n");
    }
    std::exit(1);
  }

  if (!AuthenticateContext(c, password)) {
    printf("Connection error: error authenticating\n");
    redisFree(c);
    std::exit(1);
  }

  return c;
}

bool SyncReconnect(redisContext* c, const std::string& password) {
  if (redisReconnect(c) != REDIS_OK) {
    return false;
  }
  return AuthenticateContext(c, password);
}

KeyReader::KeyReader(RedisModuleCtx* ctx, const std::string& key) : ctx_(ctx) {
  name_ = RedisModule_CreateString(ctx, key.data(), key.size());
  key_ = reinterpret_cast<RedisModuleKey*>(
      RedisModule_OpenKey(ctx, name_, REDISMODULE_READ));
}

KeyReader::KeyReader(RedisModuleCtx* ctx, RedisModuleString* key)
    : KeyReader(ctx, ReadString(key)) {}

KeyReader::~KeyReader() {
  RedisModule_CloseKey(key_);
  RedisModule_FreeString(ctx_, name_);
}
const char* KeyReader::key(size_t* size) {
  return RedisModule_StringPtrLen(name_, size);
}
const char* KeyReader::value(size_t* size) const {
  return RedisModule_StringDMA(key_, size, REDISMODULE_READ);
}
bool KeyReader::IsEmpty() const {
  return RedisModule_KeyType(key_) == REDISMODULE_KEYTYPE_EMPTY;
}
