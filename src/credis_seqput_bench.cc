#include <chrono>
#include <fstream>
#include <random>
#include <thread>
#include <unordered_set>

#include "glog/logging.h"

#include "client.h"
#include "timer.h"

// To launch with 2 servers:
//
//   pkill -f redis-server; ./setup.sh 2; make -j;
//   ./src/credis_seqput_bench 2
//
// If "2" is omitted in the above, by default 1 server is used.

const int N = 500000;
aeEventLoop* loop = aeCreateEventLoop(64);
int writes_completed = 0;
int reads_completed = 0;
Timer timer;
std::string last_issued_read_key;
// Randomness.
std::default_random_engine re;
double kWriteRatio = 1.0;

double last_unacked_timestamp = -1;

redisAsyncContext* write_context = nullptr;
redisAsyncContext* read_context = nullptr;

// Client's bookkeeping for seqnums.
std::unordered_set<int64_t> assigned_seqnums;
std::unordered_set<int64_t> acked_seqnums;

// Clients also need UniqueID support.
// TODO(zongheng): implement this properly via uuid, currently it's pid.
const std::string client_id = std::to_string(getpid());

// Forward declaration.
void SeqPutCallback(redisAsyncContext*, void*, void*);
void SeqGetCallback(redisAsyncContext*, void*, void*);
void AsyncPut();
void AsyncGet();

// Launch a GET or PUT, depending on "write_ratio".
void AsyncRandomCommand() {
  static std::uniform_real_distribution<double> unif(0.0, 1.0);
  const double r = unif(re);
  if (r < kWriteRatio || writes_completed == 0) {
    AsyncPut();
  } else {
    AsyncGet();
  }
}

void OnCompleteLaunchNext(int* cnt, int other_cnt) {
  ++(*cnt);
  timer.TimeOpEnd(*cnt + other_cnt);
  if (*cnt + other_cnt == N) {
    aeStop(loop);
    return;
  }
  // Launch next pair.
  AsyncRandomCommand();
}

// This callback gets fired whenever the store assigns a seqnum for a Put
// request.
void SeqPutCallback(redisAsyncContext* write_context,  // != ack_context.
                    void* r,
                    void*) {
  const redisReply* reply = reinterpret_cast<redisReply*>(r);
  const int64_t assigned_seqnum = reply->integer;
  // LOG(INFO) << "SeqPutCallback " << assigned_seqnum;
  auto it = acked_seqnums.find(assigned_seqnum);
  if (it != acked_seqnums.end()) {
    acked_seqnums.erase(it);
    last_unacked_timestamp = -1;
    OnCompleteLaunchNext(&writes_completed, reads_completed);
  } else {
    assigned_seqnums.insert(assigned_seqnum);
  }
}

// Put(n -> n), for n == writes_completed.
void AsyncPut() {
  const std::string data = std::to_string(writes_completed);
  // LOG(INFO) << "PUT " << data;
  last_unacked_timestamp = timer.TimeOpBegin();
  const int status = redisAsyncCommand(
      write_context, &SeqPutCallback,
      /*privdata=*/NULL, "MEMBER.PUT %b %b %b", data.data(), data.size(),
      data.data(), data.size(), client_id.data(), client_id.size());
  CHECK(status == REDIS_OK);
}

// Get(i), for a random i in [0, writes_completed).
void AsyncGet() {
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
      read_context, reinterpret_cast<redisCallbackFn*>(&SeqGetCallback),
      /*privdata=*/NULL, "GET %b", query.data(), query.size());
  CHECK(status == REDIS_OK);
}

void SeqGetCallback(redisAsyncContext* /*context*/,
                    void* r,
                    void* /*privdata*/) {
  const redisReply* reply = reinterpret_cast<redisReply*>(r);
  // LOG(INFO) << "reply type " << reply->type << "; issued get "
  //           << last_issued_read_key;
  const std::string actual = std::string(reply->str, reply->len);
  CHECK(last_issued_read_key == actual)
      << "; expected " << last_issued_read_key << " actual " << actual;
  OnCompleteLaunchNext(&reads_completed, writes_completed);
}

// This gets fired whenever an ACK from the store comes back.
void SeqPutAckCallback(redisAsyncContext* ack_context,  // != write_context.
                       void* r,
                       void* privdata) {
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
  const redisReply* reply = reinterpret_cast<redisReply*>(r);
  const redisReply* message_type = reply->element[0];

  // NOTE(zongheng): this is a hack.
  // if (strcmp(message_type->str, "message") == 0) {
  if (reply->element[2]->str == nullptr) {
    LOG(INFO) << getpid() << " subscribed";
    LOG(INFO) << getpid() << " chan: " << reply->element[1]->str;
    return;
  }
  const int64_t received_sn = std::stoi(reply->element[2]->str);

  auto it = assigned_seqnums.find(received_sn);
  if (it == assigned_seqnums.end()) {
    acked_seqnums.insert(received_sn);
    return;
  }
  // Otherwise, found & act on this ACK.
  assigned_seqnums.erase(it);
  last_unacked_timestamp = -1;
  OnCompleteLaunchNext(&writes_completed, reads_completed);
}

const double kRetryTimeoutMicrosecs = 500000;
int RetryPutTimer(aeEventLoop* loop, long long timer_id, void*) {
  const double now_us = timer.NowMicrosecs();
  const double diff = now_us - last_unacked_timestamp;
  if (last_unacked_timestamp > 0 && diff > kRetryTimeoutMicrosecs) {
    LOG(INFO) << "Retrying PUT " << writes_completed;
    LOG(INFO) << "now " << now_us << " last " << last_unacked_timestamp
              << " diff " << diff;
    AsyncPut();
  }
  return 0;  // Reset timer to 0.
}

int main(int argc, char** argv) {
  // Parse.
  int num_chain_nodes = 1;
  if (argc > 1) num_chain_nodes = std::stoi(argv[1]);
  if (argc > 2) kWriteRatio = std::stod(argv[2]);
  // Set up "write_port" and "ack_port".
  const int write_port = 6370;
  const int ack_port = write_port + num_chain_nodes - 1;
  LOG(INFO) << "num_chain_nodes " << num_chain_nodes << " write_port "
            << write_port << " ack_port " << ack_port << " write_ratio "
            << kWriteRatio;

  RedisClient client;
  CHECK(client.Connect("127.0.0.1", write_port, ack_port).ok());
  CHECK(client.AttachToEventLoop(loop).ok());
  CHECK(client
            .RegisterAckCallback(
                static_cast<redisCallbackFn*>(&SeqPutAckCallback))
            .ok());
  write_context = client.write_context();
  read_context = client.read_context();

  // Timings related.
  timer.ExpectOps(N);

  aeCreateTimeEvent(loop, /*milliseconds=*/1, &RetryPutTimer, NULL, NULL);

  std::this_thread::sleep_for(std::chrono::seconds(1));
  LOG(INFO) << "starting bench";
  auto start = std::chrono::system_clock::now();

  // Start with first query, and each callback will launch the next.
  AsyncRandomCommand();

  LOG(INFO) << "start loop";
  aeMain(loop);
  LOG(INFO) << "end loop";

  auto end = std::chrono::system_clock::now();
  CHECK(writes_completed + reads_completed == N)
      << "writes_completed + reads_completed "
      << writes_completed + reads_completed << " vs N " << N;
  LOG(INFO) << "ending bench";

  const int64_t latency_us =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  LOG(INFO) << "throughput " << N * 1e6 / latency_us
            << " ops/s, total duration (ms) " << latency_us / 1e3 << ", num "
            << N << ", write_ratio " << kWriteRatio;

  // Timings related.
  double mean = 0, std = 0;
  timer.Stats(&mean, &std);
  LOG(INFO) << "latency (us) mean " << mean << " std " << std;
  {
    std::ofstream ofs("latency.txt");
    for (double x : timer.latency_micros()) ofs << x << std::endl;
  }

  return 0;
}
