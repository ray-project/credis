#ifndef CREDIS_REDIS_DECLS_H_
#define CREDIS_REDIS_DECLS_H_

// Declaration-only header file for things that we make use of from
// redismodule.h.

// NOTE: these are pulled from / should be in sync with redismodule.h.
extern "C" {

#define REDISMODULE_API_FUNC(x) (*x)

#define REDISMODULE_KEYTYPE_EMPTY 0
#define REDISMODULE_READ (1 << 0)

typedef struct RedisModuleCtx RedisModuleCtx;
typedef struct RedisModuleKey RedisModuleKey;
typedef struct RedisModuleString RedisModuleString;

extern void* REDISMODULE_API_FUNC(RedisModule_OpenKey)(
    RedisModuleCtx* ctx, RedisModuleString* keyname, int mode);
extern void REDISMODULE_API_FUNC(RedisModule_CloseKey)(RedisModuleKey* kp);
extern RedisModuleString* REDISMODULE_API_FUNC(RedisModule_CreateString)(
    RedisModuleCtx* ctx, const char* ptr, size_t len);
extern char* REDISMODULE_API_FUNC(RedisModule_StringDMA)(RedisModuleKey* key,
                                                         size_t* len, int mode);
extern const char* REDISMODULE_API_FUNC(RedisModule_StringPtrLen)(
    const RedisModuleString* str, size_t* len);
extern void REDISMODULE_API_FUNC(RedisModule_FreeString)(
    RedisModuleCtx* ctx, RedisModuleString* str);
extern int REDISMODULE_API_FUNC(RedisModule_KeyType)(RedisModuleKey* kp);
extern int REDISMODULE_API_FUNC(RedisModule_Publish)(
    RedisModuleString* channel, RedisModuleString* message);
}

#endif  //  CREDIS_REDIS_DECLS_H_
