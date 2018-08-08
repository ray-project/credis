#include <chrono>
#include <vector>

#include "glog/logging.h"

#include "client.h"

const int N = 5000000;
int num_completed = 0;
int num_successful = 0;
aeEventLoop* loop = aeCreateEventLoop(1024);
void ParPutCallback(redisAsyncContext* c, void*, void*) {
  if (c->err) {
    LOG(ERROR) << "Error: " << c->errstr;
  } else {
    ++num_successful;
  }
  ++num_completed;
  if (num_completed == N) {
    aeStop(loop);
  }
}

int main() {
  RedisClient client;
  client.Connect("127.0.0.1", 6370);
  client.AttachToEventLoop(loop);
  redisAsyncContext* context = client.write_context();

  LOG(INFO) << "starting bench";
  auto start = std::chrono::system_clock::now();
  // Parallel put: launch N writes together.
  for (int i = 0; i < N; ++i) {
    const std::string i_str = std::to_string(i);
    const int status = redisAsyncCommand(
        context, reinterpret_cast<redisCallbackFn*>(&ParPutCallback),
        /*privdata=*/NULL, "SET %b %b", i_str.data(), i_str.size(),
        i_str.data(), i_str.size());
    CHECK(status == REDIS_OK);
  }
  aeMain(loop);
  auto end = std::chrono::system_clock::now();
  CHECK(num_completed == N) << "num_completed " << num_completed << " vs N "
                            << N;
  CHECK(num_successful == N) << "num_successful " << num_successful << " vs N "
                             << N;
  LOG(INFO) << "ending bench";

  const int64_t latency_us =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  LOG(INFO) << "throughput " << N * 1e6 / latency_us
            << " writes/s, total duration (ms) " << latency_us / 1e3 << ", num "
            << N;

  return 0;
}
