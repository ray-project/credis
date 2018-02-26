#include <chrono>
#include <fstream>
#include <random>
#include <vector>

#include "glog/logging.h"

#include "client.h"
#include "timer.h"

const int N = 50000;
// const int N = 500000;
aeEventLoop* loop = aeCreateEventLoop(64);
int writes_completed = 0;
int reads_completed = 0;
Timer reads_timer, writes_timer;
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
  writes_timer.TimeOpEnd(writes_completed);
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
  CHECK(last_issued_read_key == actual) << "; expected " << last_issued_read_key
                                        << " actual " << actual;
  reads_timer.TimeOpEnd(reads_completed);
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
  writes_timer.TimeOpBegin();
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

  reads_timer.TimeOpBegin();
  // LOG(INFO) << "GET " << query;
  const int status = redisAsyncCommand(
      context, reinterpret_cast<redisCallbackFn*>(&SeqGetCallback),
      /*privdata=*/NULL, "GET %b", query.data(), query.size());
  CHECK(status == REDIS_OK);
}

int main(int argc, char** argv) {
  // Parse.
  if (argc > 1) kWriteRatio = std::stod(argv[1]);
  std::string server = "127.0.0.1";
  if (argc > 2) server = std::string(argv[2]);

  RedisClient client;
  client.Connect(server, 6370);
  client.AttachToEventLoop(loop);
  redisAsyncContext* context = client.write_context();

  reads_timer.ExpectOps(N);
  writes_timer.ExpectOps(N);
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

  // Timings related.
  Timer merged = Timer::Merge(reads_timer, writes_timer);
  double composite_mean = 0, composite_std = 0;
  merged.Stats(&composite_mean, &composite_std);
  double reads_mean = 0, reads_std = 0;
  reads_timer.Stats(&reads_mean, &reads_std);
  double writes_mean = 0, writes_std = 0;
  writes_timer.Stats(&writes_mean, &writes_std);

  LOG(INFO) << "throughput " << N * 1e6 / latency_us
            << " ops/s, total duration (ms) " << latency_us / 1e3 << ", num "
            << N << ", write_ratio " << kWriteRatio;
  LOG(INFO) << "reads_thput "
            << reads_completed * 1e6 / (reads_mean * reads_completed)
            << " ops/s, total duration(ms) "
            << (reads_mean * reads_completed) / 1e3 << ", num "
            << reads_completed;
  LOG(INFO) << "writes_thput "
            << writes_completed * 1e6 / (writes_mean * writes_completed)
            << " ops/s, total duration(ms) "
            << (writes_mean * writes_completed) / 1e3 << ", num "
            << writes_completed;

  LOG(INFO) << "latency (us) mean " << composite_mean << " std "
            << composite_std;
  LOG(INFO) << "reads_lat (us) mean " << reads_mean << " std " << reads_std;
  LOG(INFO) << "writes_lat (us) mean " << writes_mean << " std " << writes_std;
  // {
  //   std::ofstream ofs("redis-latency.txt");
  //   for (double x : timer.latency_micros()) ofs << x << std::endl;
  // }

  return 0;
}
