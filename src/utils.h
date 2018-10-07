#ifndef CREDIS_UTILS_H_
#define CREDIS_UTILS_H_

#include <string>

#include "redis_decls.h"

extern "C" {
#include "hiredis/hiredis.h"
}

// Convert RedisModuleString to C++ string.
const char* ReadString(const RedisModuleString* str, size_t* len);
std::string ReadString(RedisModuleString* str);

// On success, returns a synchronous redisContext client; else exit(1).
redisContext* SyncConnect(const std::string& address, int port,
                          const std::string& password);

// Reconnects and re-authenticates a synchronous redisContext client.
bool SyncReconnect(redisContext* c, const std::string& password);

// Helper class to read data from a key and handle closing the key in an
// appropriate way.
class KeyReader {
 public:
  KeyReader(RedisModuleCtx* ctx, const std::string& key);
  KeyReader(RedisModuleCtx* ctx, RedisModuleString* key);
  ~KeyReader();

  const char* key(size_t* size);
  const char* value(size_t* size) const;
  bool IsEmpty() const;

 private:
  RedisModuleCtx* ctx_;
  RedisModuleString* name_;
  RedisModuleKey* key_;
};

#endif  // CREDIS_UTILS_H_
