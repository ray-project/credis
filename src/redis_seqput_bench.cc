#include <chrono>
#include <vector>

#include "glog/logging.h"

#include "client.h"

const int N = 500000;
int num_completed = 0;
aeEventLoop* loop = aeCreateEventLoop(64);
void SeqPutCallback(redisAsyncContext* context, void*, void*) {
  ++num_completed;
  if (num_completed == N) {
    aeStop(loop);
    return;
  }
  // Launch next pair.
  const std::string s = std::to_string(num_completed);
  const int status = redisAsyncCommand(
      context, reinterpret_cast<redisCallbackFn*>(&SeqPutCallback),
      /*privdata=*/NULL, "SET %b %b", s.data(), s.size(), s.data(), s.size());
  CHECK(status == REDIS_OK);
}

int main() {
  RedisClient client;
  client.Connect("127.0.0.1", 6370);
  client.AttachToEventLoop(loop);
  redisAsyncContext* context = client.async_context();

  LOG(INFO) << "starting bench";
  auto start = std::chrono::system_clock::now();

  // SeqPut.  Start with "0->0", and each callback will launch the next pair.
  const std::string kZeroStr = "0";
  const int status = redisAsyncCommand(
      context, reinterpret_cast<redisCallbackFn*>(&SeqPutCallback),
      /*privdata=*/NULL, "SET %b %b", kZeroStr.data(), kZeroStr.size(),
      kZeroStr.data(), kZeroStr.size());
  CHECK(status == REDIS_OK);

  aeMain(loop);

  auto end = std::chrono::system_clock::now();
  CHECK(num_completed == N)
      << "num_completed " << num_completed << " vs N " << N;
  LOG(INFO) << "ending bench";

  const int64_t latency_us =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  LOG(INFO) << "throughput " << N * 1e6 / latency_us
            << " writes/s, total duration (ms) " << latency_us / 1e3 << ", num "
            << N;

  return 0;
}
