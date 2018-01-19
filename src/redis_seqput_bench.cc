#include <chrono>
#include <fstream>
#include <random>
#include <vector>

#include "glog/logging.h"

#include "client.h"
#include "timer.h"

const int N = 500000;
aeEventLoop* loop = aeCreateEventLoop(64);
int writes_completed = 0;
int reads_completed = 0;
Timer timer;
std::string last_issued_read_key;
// Randomness.
std::default_random_engine re;
double kWriteRatio = 1.0;

// Forward declaration.
void AsyncPut(redisAsyncContext*);
void AsyncGet(redisAsyncContext*);
void AsyncRandomCommand(redisAsyncContext*);

void SeqPutCallback(redisAsyncContext* context, void*, void*) {
  ++writes_completed;
  timer.TimeOpEnd(writes_completed + reads_completed);
  if (writes_completed + reads_completed == N) {
    aeStop(loop);
    return;
  }
  // Launch next pair.
  AsyncRandomCommand(context);
}

void SeqGetCallback(redisAsyncContext* context, void* r, void* /*privdata*/) {
  ++reads_completed;
  const redisReply* reply = reinterpret_cast<redisReply*>(r);
  // LOG(INFO) << "reply type " << reply->type << "; issued get "
  //           << last_issued_read_key;
  const std::string actual = std::string(reply->str, reply->len);
  CHECK(last_issued_read_key == actual)
      << "; expected " << last_issued_read_key << " actual " << actual;
  timer.TimeOpEnd(writes_completed + reads_completed);
  if (writes_completed + reads_completed == N) {
    aeStop(loop);
    return;
  }

  // Launch next pair.
  AsyncRandomCommand(context);
}

// Launch a GET or PUT, depending on "write_ratio".
void AsyncRandomCommand(redisAsyncContext* context) {
  static std::uniform_real_distribution<double> unif(0.0, 1.0);
  const double r = unif(re);
  if (r < kWriteRatio || writes_completed == 0) {
    AsyncPut(context);
  } else {
    AsyncGet(context);
  }
}

// Put(n -> n), for n == writes_completed.
void AsyncPut(redisAsyncContext* context) {
  const std::string data = std::to_string(writes_completed);
  // LOG(INFO) << "PUT " << data;
  timer.TimeOpBegin();
  const int status = redisAsyncCommand(
      context, reinterpret_cast<redisCallbackFn*>(&SeqPutCallback),
      /*privdata=*/NULL, "SET %b %b", data.data(), data.size(), data.data(),
      data.size());
  CHECK(status == REDIS_OK);
}

// Get(i), for a random i in [0, writes_completed).
void AsyncGet(redisAsyncContext* context) {
  CHECK(writes_completed > 0);
  std::uniform_int_distribution<> unif_int(0, writes_completed - 1);
  const int r = unif_int(re);
  // LOG(INFO) << "random int " << r << " writes_completed " <<
  // writes_completed;
  const std::string query = std::to_string(r);
  last_issued_read_key = query;

  timer.TimeOpBegin();
  // LOG(INFO) << "GET " << query;
  const int status = redisAsyncCommand(
      context, reinterpret_cast<redisCallbackFn*>(&SeqGetCallback),
      /*privdata=*/NULL, "GET %b", query.data(), query.size());
  CHECK(status == REDIS_OK);
}

int main(int argc, char** argv) {
  // Parse.
  if (argc > 1) kWriteRatio = std::stod(argv[1]);

  RedisClient client;
  client.Connect("127.0.0.1", 6370);
  client.AttachToEventLoop(loop);
  redisAsyncContext* context = client.write_context();

  timer.ExpectOps(N);
  LOG(INFO) << "starting bench, write_ratio: " << kWriteRatio;
  auto start = std::chrono::system_clock::now();

  // Start with Put(0->0) (or GET(0)), and each callback will launch the next
  // pair.
  AsyncRandomCommand(context);
  aeMain(loop);

  auto end = std::chrono::system_clock::now();
  CHECK(writes_completed + reads_completed == N);
  LOG(INFO) << "ending bench";

  const int64_t latency_us =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  LOG(INFO) << "throughput " << N * 1e6 / latency_us
            << " ops/s, total duration (ms) " << latency_us / 1e3 << ", num "
            << N << ", write_ratio " << kWriteRatio;

  double mean = 0, std = 0;
  timer.Stats(&mean, &std);
  LOG(INFO) << "latency (us) mean " << mean << " std " << std;
  {
    std::ofstream ofs("redis-latency.txt");
    for (double x : timer.latency_micros()) ofs << x << std::endl;
  }

  return 0;
}
