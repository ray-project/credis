#include <algorithm>
#include <chrono>
#include <fstream>
#include <random>
#include <thread>
#include <unordered_set>

#include <unistd.h>

#include "glog/logging.h"

#include "client.h"
#include "master_client.h"
#include "timer.h"

// Fixed-size keys and values.  Works with wr = 1 currently.
static const int kKeySize = 25;
static const int kValueSize = 512;

std::string random_string(size_t length) {
  auto randchar = []() -> char {
    const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = (sizeof(charset) - 1);
    return charset[rand() % max_index];
  };
  std::string str(length, 0);
  std::generate_n(str.begin(), length, randchar);
  return str;
}

// TODO(zongheng): timeout should be using exponential backoff and/or some
// randomization; this is critical in distributed settings (e.g., multiple
// processes running this same program) to avoid catastrophic failures.

// To launch with 2 servers:
//
//   pkill -f redis-server; ./setup.sh 2; make -j;
//   ./src/credis_seqput_bench 2
//
// If "2" is omitted in the above, by default 1 server is used.

// const int N = 1000000;

// ~70 sec.
int N = 200000;

// const int N = 60000;

// 1-chain ~5sec, 2-chain ~8sec.
// const int N = 20000;

aeEventLoop* loop = aeCreateEventLoop(64);
int writes_completed = 0;
int reads_completed = 0;
Timer reads_timer, writes_timer;
std::string last_issued_read_key;
// Randomness.
std::default_random_engine re;
double kWriteRatio = 1.0;

double last_unacked_timestamp = -1;
int64_t last_unacked_seqnum = -1;

redisAsyncContext* write_context = nullptr;
redisAsyncContext* read_context = nullptr;

// Client's bookkeeping for seqnums.
std::unordered_set<int64_t> assigned_seqnums;
std::unordered_set<int64_t> acked_seqnums;

std::shared_ptr<RedisClient> client;
std::shared_ptr<RedisMasterClient> master_client;

// Clients also need UniqueID support.
// TODO(zongheng): implement this properly via uuid, currently it's pid.
const std::string client_id = std::to_string(getpid());

// Forward declaration.
void SeqPutCallback(redisAsyncContext*, void*, void*);
void SeqGetCallback(redisAsyncContext*, void*, void*);
void AsyncPut(bool);
void AsyncGet();
void AsyncNoReply();

// Launch a GET or PUT, depending on "write_ratio".
void AsyncRandomCommand() {
  // AsyncNoReply();

  static std::uniform_real_distribution<double> unif(0.0, 1.0);
  const double r = unif(re);
  if (r < kWriteRatio || writes_completed == 0) {
    AsyncPut(/*is_retry=*/false);
  } else {
    AsyncGet();
  }
}

void OnCompleteLaunchNext(Timer* timer, int* cnt, int other_cnt) {
  // Sometimes an ACK comes back late, just ignore if we're done.
  if (*cnt + other_cnt >= N) return;
  ++(*cnt);
  timer->TimeOpEnd(*cnt);
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
                    void* r, void*) {
  if (r == nullptr) {
    // It is possible to be given nullptr as the reply.  For instance, a Put
    // that gets ignored with no reply being sent; on program exit, the context
    // runs its cleanup code, and __redisRunCallback can generate this nullptr
    // callback; this is empircally observed.
    return;
  }
  const redisReply* reply = reinterpret_cast<redisReply*>(r);
  const int64_t assigned_seqnum = reply->integer;
  // LOG(INFO) << "SeqPutCallback " << assigned_seqnum;
  auto it = acked_seqnums.find(assigned_seqnum);
  if (it != acked_seqnums.end()) {
    acked_seqnums.erase(it);
    last_unacked_timestamp = -1;
    last_unacked_seqnum = -1;
    OnCompleteLaunchNext(&writes_timer, &writes_completed, reads_completed);
  } else {
    DLOG(INFO) << "assigning last_unacked_seqnum with value "
               << assigned_seqnum;
    // This is a contract with the store.  Even if the store drops writes during
    // anomaly repair, it should return nothing for the dropped writes.  (This
    // seems to be a better choice than returning a special value like -1, as in
    // the future we might consider shifting the burden of generating seqnum to
    // the client.  Under that setting, I think the client should be able to
    // generate arbitrary seqnums, not just nonnegative numbers.  Although,
    // hiredis / redis might have undiscovered issues with a redis module
    // command not replying anything...)
    CHECK(assigned_seqnum >= writes_completed)
        << assigned_seqnum << " " << writes_completed;
    last_unacked_seqnum = assigned_seqnum;
    assigned_seqnums.insert(assigned_seqnum);
  }
}

// Put(n -> n), for n == writes_completed.
void AsyncPut(bool is_retry) {
  // i -> i.
  const std::string data = std::to_string(writes_completed);

  const std::string key = random_string(kKeySize);
  const std::string value = random_string(kValueSize);

  // LOG(INFO) << "PUT " << data;
  if (!is_retry) {
    last_unacked_timestamp = writes_timer.TimeOpBegin();
  }
  const int status = redisAsyncCommand(
      write_context, &SeqPutCallback,
      /*privdata=*/NULL, "MEMBER.PUT %b %b %b", key.data(), key.size(),
      value.data(), value.size(), client_id.data(), client_id.size());
  // /*privdata=*/NULL, "MEMBER.PUT %b %b %b", data.data(), data.size(),
  //   data.data(), data.size(), client_id.data(), client_id.size());
  CHECK(status == REDIS_OK);
}

void AsyncNoReplyCallback(redisAsyncContext* write_context,  // != ack_context.
                          void* r, void*) {
  CHECK(0) << "Should never be called";
}
void AsyncNoReply() {
  const int status =
      redisAsyncCommand(write_context, /*callback=*/&AsyncNoReplyCallback,
                        /*privdata=*/NULL, "NOREPLY");
  CHECK(status == REDIS_OK);
  LOG(INFO) << "Done issuing AsyncNoReply";
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

  reads_timer.TimeOpBegin();
  // LOG(INFO) << "GET " << query;
  const int status = redisAsyncCommand(
      read_context, reinterpret_cast<redisCallbackFn*>(&SeqGetCallback),
      /*privdata=*/NULL, "GET %b", query.data(), query.size());
  CHECK(status == REDIS_OK);
}

void SeqGetCallback(redisAsyncContext* /*context*/, void* r,
                    void* /*privdata*/) {
  const redisReply* reply = reinterpret_cast<redisReply*>(r);
  // LOG(INFO) << "reply type " << reply->type << "; issued get "
  //           << last_issued_read_key;
  const std::string actual = std::string(reply->str, reply->len);
  CHECK(last_issued_read_key == actual)
      << "; expected " << last_issued_read_key << " actual " << actual;
  OnCompleteLaunchNext(&reads_timer, &reads_completed, writes_completed);
}

// This gets fired whenever an ACK from the store comes back.
void SeqPutAckCallbackHelper(
    redisAsyncContext* ack_context,  // != write_context.
    void* r, void* privdata, bool issue_first_cmd) {
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
  if (r == nullptr) {
    DLOG(INFO)
        << "Received null reply, ignoring (could happen upon the removal "
           "or reconnection of fault server nodes)";
    // NOTE(zongheng): accessing ack_context at all might cause segfaults. Even
    // for (ack_context == nullptr) test.  Weird.
    return;
  }
  const redisReply* reply = reinterpret_cast<redisReply*>(r);

  // NOTE(zongheng): this check is a hack to see if the msg is a
  // subscribe-success message.
  if (reply->element[2]->str == nullptr) {
    LOG(INFO) << getpid() << " subscribed";
    LOG(INFO) << getpid() << " chan: " << reply->element[1]->str;
    if (issue_first_cmd) {
      AsyncRandomCommand();
    }
    return;
  }
  const int64_t received_sn = std::stoi(reply->element[2]->str);

  auto it = assigned_seqnums.find(received_sn);
  if (it == assigned_seqnums.end()) {
    // LOG(INFO) << "seqnum acked " << received_sn;
    acked_seqnums.insert(received_sn);
    return;
  }
  // Otherwise, found & act on this ACK.
  DLOG(INFO) << "seqnum acked " << received_sn
             << "; setting last_unacked_{timestamp,seqnum} to -1";
  assigned_seqnums.erase(it);
  last_unacked_timestamp = -1;
  last_unacked_seqnum = -1;
  OnCompleteLaunchNext(&writes_timer, &writes_completed, reads_completed);
}

void SeqPutAckCallback(redisAsyncContext* ack_context,  // != write_context.
                       void* r, void* privdata) {
  return SeqPutAckCallbackHelper(ack_context, r, privdata,
                                 /*issue_first_cmd=*/true);
}
void SeqPutAckCallbackAfterRefresh(
    redisAsyncContext* ack_context,  // != write_context.
    void* r, void* privdata) {
  return SeqPutAckCallbackHelper(ack_context, r, privdata,
                                 /*issue_first_cmd=*/false);
}

// TODO(zongheng): if same "writes_completed" stalled for more than X times,
// e.g. 3, trigger refresh tail -> ConnectContext(new_tail_port) ->
// RegisterAckCallback(new_tail)

// Fires at this frequency.
const long long kRetryTimerMillisecs = 100;  // For ae's timer.
// Represents the timeout which if exceeded, we retry last unacked command.
const double kRetryTimeoutMicrosecs = 1 * 1e5;  // 100 ms
// const double kRetryTimeoutMicrosecs = 1 * 1e6; // 1 sec
// const double kRetryTimeoutMicrosecs = 5 * 1e6; // 5 sec

const long long kRefreshTailTimerMillisecs = 100;
// A value of "N * kRetryTimeoutMicrosecs" here means (N - 1) retries have
// been issued, none are heard back.  Must be N > 1.
//
// This knob normally forms the upper bound on
// time-from-first-retry-to-next-success.
const double kRefreshTailPutThresholdMicrosecs = 2 * kRetryTimeoutMicrosecs;

// TODO(zongheng): what's to prevent us from reconnecting to the tail in a loop,
// except by luck?
int RefreshTailTimer(aeEventLoop* loop, long long /*timer_id*/, void*) {
  const double now_us = writes_timer.NowMicrosecs();
  const double diff = now_us - last_unacked_timestamp;
  if (last_unacked_timestamp == -1 ||
      diff < kRefreshTailPutThresholdMicrosecs) {
    // LOG(INFO) << "RefreshTailTimer: last_unacked_timestamp "
    //           << last_unacked_timestamp << " diff " << diff;
    // Fire again at this distance from now.
    return kRefreshTailTimerMillisecs;
  }

  DLOG(INFO) << "In RefreshTailTimer";

  // Otherwise, ask master for new tail, and connect to it.
  std::string tail_address;
  int tail_port;
  DLOG(INFO) << "Issuing Tail()";
  CHECK(master_client->Tail(&tail_address, &tail_port).ok());
  DLOG(INFO) << "Issuing ReconnectAckContext()";
  CHECK(client
            ->ReconnectAckContext(
                tail_address, tail_port,
                static_cast<redisCallbackFn*>(&SeqPutAckCallbackAfterRefresh))
            .ok());
  DLOG(INFO) << "AckContext reconnected, port: "
             << client->read_context()->c.tcp.port;

  // Fire again at this distance from now.
  // We choose to lengthen this window to not uncessarily refresh the tail too
  // frequently.  Using 1x results in one (observed) extra refresh in some
  // setting.
  return kRefreshTailTimerMillisecs * 2;
}

int RetryPutTimer(aeEventLoop* loop, long long /*timer_id*/, void*) {
  const double now_us = writes_timer.NowMicrosecs();
  const double diff = now_us - last_unacked_timestamp;
  if (last_unacked_timestamp > 0 && diff > kRetryTimeoutMicrosecs) {
    LOG(INFO) << "Retrying PUT, writes_completed " << writes_completed;
    LOG(INFO) << " time diff (us) " << diff << "; last_unacked_seqnum "
              << last_unacked_seqnum;
    // If the ACK for "last_unacked_seqnum" comes back later, we should ignore
    // it, since we are about to retry and will associate a new seqnum to the
    // retried op.
    if (last_unacked_seqnum >= 0) {
      auto it = assigned_seqnums.find(last_unacked_seqnum);
      CHECK(it != assigned_seqnums.end());
      assigned_seqnums.erase(it);
      // We do not assign -1 to "last_unacked_timestamp" because it should
      // capture the original op's start time.
      last_unacked_seqnum = -1;
    }
    AsyncPut(/*is_retry=*/true);

    // return kRetryTimeoutMicrosecs;
  }
  return kRetryTimerMillisecs;  // Fire at this distance from now.
}

int main(int argc, char** argv) {
  // Args:
  //   num_chain_nodes kWriteRatio head_server N tail_server
  // All optional.

  int num_chain_nodes = 1;
  if (argc > 1) num_chain_nodes = std::stoi(argv[1]);
  if (argc > 2) kWriteRatio = std::stod(argv[2]);
  int write_port = 6370;
  int ack_port = write_port + num_chain_nodes - 1;
  std::string head_server = "127.0.0.1", tail_server = "127.0.0.1";
  if (argc > 3) head_server = std::string(argv[3]);
  if (argc > 4) N = std::stoi(argv[4]);
  if (argc > 5) {
    tail_server = std::string(argv[5]);
    CHECK(num_chain_nodes == 2);
    ack_port = 6370;
  }
  LOG(INFO) << "num_chain_nodes " << num_chain_nodes << " write_port "
            << write_port << " ack_port " << ack_port << " write_ratio "
            << kWriteRatio << " head_server " << head_server << " tail_server "
            << tail_server;

  client = std::make_shared<RedisClient>();

  CHECK(client->ConnectHead(head_server, write_port).ok());
  CHECK(client->ConnectTail(tail_server, ack_port).ok());

  CHECK(client->AttachToEventLoop(loop).ok());
  master_client = std::make_shared<RedisMasterClient>();
  CHECK(
      master_client->Connect(head_server, 6369).ok());  // TODO(zongheng): arg.

  // NOTE(zongheng): RegisterAckCallback() subscribes ME to a channel.
  // SeqPutAckCallback is responsible for listening for:
  //
  // (1) (setup) on subscribe success;
  // (2) (normal state) the ack for every Put.
  //
  // On receipt of (1): it's responsible for firing the first command, via
  // AsyncRandomCommand(). This is critical for correctness as if we issue the
  // first command prior to knowing for sure we're subscribed, we could be
  // missing the initial ACKs, causing unnecessary retries.
  //
  // On receipt of (2): it issues another call to AsyncRandomCommand().  Hence
  // this client program is sequential.
  CHECK(client
            ->RegisterAckCallback(
                static_cast<redisCallbackFn*>(&SeqPutAckCallback))
            .ok());

  write_context = client->write_context();
  read_context = client->read_context();

  // Timings related.
  reads_timer.ExpectOps(N);
  writes_timer.ExpectOps(N);

  aeCreateTimeEvent(loop, /*milliseconds=*/kRetryTimerMillisecs, &RetryPutTimer,
                    /*clientData=*/NULL, /*finalizerProc=*/NULL);
  aeCreateTimeEvent(loop, /*milliseconds=*/kRefreshTailTimerMillisecs,
                    &RefreshTailTimer, /*clientData=*/NULL,
                    /*finalizerProc=*/NULL);

  std::this_thread::sleep_for(std::chrono::seconds(1));
  LOG(INFO) << "starting bench";
  auto start = std::chrono::system_clock::now();

  // LOG(INFO) << "start loop";
  aeMain(loop);
  // LOG(INFO) << "end loop";

  auto end = std::chrono::system_clock::now();
  CHECK(writes_completed + reads_completed == N);
  LOG(INFO) << "ending bench";
  const int64_t latency_us =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();

  // Timings related.
  Timer merged = Timer::Merge(reads_timer, writes_timer);
  merged.DropFirst(10);

  double composite_mean = 0, composite_std = 0;
  merged.Stats(&composite_mean, &composite_std);
  double reads_mean = 0, reads_std = 0;
  reads_timer.Stats(&reads_mean, &reads_std);
  double writes_mean = 0, writes_std = 0;
  writes_timer.Stats(&writes_mean, &writes_std);

  const int pid = getpid();
  const std::string pathname = "client-" + std::to_string(pid) + "-" +
                               std::to_string(num_chain_nodes) + "nodes.csv";
  merged.WriteToFile(pathname);

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

  LOG(INFO) << "pid " << pid;
  return 0;
}
