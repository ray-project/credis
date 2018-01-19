// TODO(zongheng): this benchmark is likely outdated and needs fixes.

#include <chrono>
#include <cmath>
#include <vector>

#include "glog/logging.h"

#include "client.h"

aeEventLoop* loop = aeCreateEventLoop(1024);
redisAsyncContext* write_context = nullptr;
const int N = 500000;
int num_completed = 0;
using TimePoint = std::chrono::time_point<std::chrono::system_clock>;
std::vector<TimePoint> time_starts;
std::vector<float> latencies;

namespace {

float Sum(const std::vector<float>& xs) {
  float sum = 0;
  for (float x : xs) sum += x;
  return sum;
}

float Std(const std::vector<float>& xs, float mean) {
  float c = 0;
  for (float x : xs) c += (mean - x) * (mean - x);
  return std::sqrt(c / xs.size());
}

}  // namespace

void ParallelPutAckCallback(redisAsyncContext* c, void* r, void* privdata) {
  const TimePoint now = std::chrono::system_clock::now();
  const redisReply* reply = reinterpret_cast<redisReply*>(r);

  /* Replies to the SUBSCRIBE command have 3 elements. There are two
   * possibilities. Either the reply is the initial acknowledgment of the
   * subscribe command, or it is a message. If it is the initial acknowledgment,
   * then
   *     - reply->element[0]->str is "subscribe"
   *     - reply->element[1]->str is the name of the channel
   *     - reply->emement[2]->str is null.
   * If it is an actual message, then
   *     - reply->element[0]->str is "message"
   *     - reply->element[1]->str is the name of the channel
   *     - reply->emement[2]->str is the contents of the message.
   */
  // CHECK(reply->type == REDIS_REPLY_ARRAY);
  // CHECK(reply->elements == 3);
  const redisReply* message_type = reply->element[0];
  if (strcmp(message_type->str, "message") == 0) {
    const int seqnum = std::stoi(reply->element[2]->str);
    latencies[seqnum] =
        std::chrono::duration<float, std::micro>(now - time_starts[seqnum])
            .count();
    ++num_completed;
    if (num_completed == N) {
      aeStop(loop);
      return;
    }
  } else if (strcmp(message_type->str, "subscribe") == 0) {
  } else {
    CHECK(false) << message_type->str;
  }
}

int main() {
  RedisClient client;
  client.Connect("127.0.0.1", 6370);
  client.AttachToEventLoop(loop);
  client.RegisterAckCallback(&ParallelPutAckCallback);
  write_context = client.write_context();

  int num_calls = 0;
  // const int N = 5000;
  time_starts.resize(N);
  latencies.resize(N);

  LOG(INFO) << "starting bench";
  auto start = std::chrono::system_clock::now();

  // Parallel put: launch N writes together.
  for (int i = 0; i < N; ++i) {
    const std::string i_str = std::to_string(i);
    const int status =
        redisAsyncCommand(write_context, /*callback=*/NULL,
                          /*privdata=*/NULL, "MEMBER.PUT %b %b", i_str.data(),
                          i_str.size(), i_str.data(), i_str.size());
    CHECK(status == REDIS_OK);

    // const int64_t callback_index = RedisCallbackManager::instance().add(
    //     [loop, &num_calls](const std::string& unused_data) {
    //       ++num_calls;
    //       // if (num_calls == N) {
    //       //   aeStop(loop);
    //       // }
    //     });
    // time_starts[i] = std::chrono::system_clock::now();
    // client.RunAsync("MEMBER.PUT", i_str, i_str.data(), i_str.size(),
    //                 callback_index);
  }
  LOG(INFO) << "starting loop";
  aeMain(loop);
  LOG(INFO) << "ending loop";
  auto end = std::chrono::system_clock::now();
  CHECK(num_completed == N)
      << "num_completed " << num_completed << " vs N " << N;

  const float latency_sum = Sum(latencies);
  const float latency_mean = latency_sum / N;
  const float latency_std = Std(latencies, latency_mean);

  const int64_t latency_us =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  LOG(INFO) << "latency_sum " << latency_sum;
  LOG(INFO) << "throughput " << N * 1e6 / latency_us
            << " writes/s, latency (us) " << latency_us * 1.0 / N << ", num "
            << N << "; mean " << latency_mean << ", std " << latency_std;

  aeDeleteEventLoop(loop);
  return 0;
}
