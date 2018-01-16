#include <chrono>
#include <thread>
#include <unordered_set>
#include <vector>

#include "glog/logging.h"

#include "client.h"

// To launch with 2 servers:
//
//   pkill -f redis-server; ./setup.sh 2; make -j;
//   ./src/credis_seqput_bench 2
//
// If "2" is omitted in the above, by default 1 server is used.

const int N = 500000;
int num_completed = 0;
aeEventLoop* loop = aeCreateEventLoop(64);
redisAsyncContext* write_context = nullptr;

// Client's bookkeeping for seqnums.
std::unordered_set<int64_t> assigned_seqnums;
std::unordered_set<int64_t> acked_seqnums;

// Clients also need UniqueID support.
// TODO(zongheng): implement this properly via uuid, currently it's pid.
const std::string client_id = std::to_string(getpid());

// Forward declaration.
void SeqPutCallback(redisAsyncContext*, void*, void*);

void OnCompleteLaunchNext() {
  ++num_completed;
  if (num_completed == N) {
    aeStop(loop);
    return;
  }

  // Launch next pair.
  const std::string s = std::to_string(num_completed);
  const int status = redisAsyncCommand(write_context, &SeqPutCallback,
                                       /*privdata=*/NULL, "MEMBER.PUT %b %b %b",
                                       s.data(), s.size(), s.data(), s.size(),
                                       client_id.data(), client_id.size());
  CHECK(status == REDIS_OK);
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
    OnCompleteLaunchNext();
  } else {
    assigned_seqnums.insert(assigned_seqnum);
  }
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
  OnCompleteLaunchNext();
}

int main(int argc, char** argv) {
  // Parse.
  int num_chain_nodes = 1;
  if (argc > 1) num_chain_nodes = std::stoi(argv[1]);
  // Set up "write_port" and "ack_port".
  const int write_port = 6370;
  const int ack_port = write_port + num_chain_nodes - 1;
  LOG(INFO) << "num_chain_nodes " << num_chain_nodes << " write_port "
            << write_port << " ack_port " << ack_port;

  RedisClient client;
  CHECK(client.Connect("127.0.0.1", write_port, ack_port).ok());
  CHECK(client.AttachToEventLoop(loop).ok());
  CHECK(client
            .RegisterAckCallback(
                static_cast<redisCallbackFn*>(&SeqPutAckCallback))
            .ok());
  write_context = client.async_context();

  std::this_thread::sleep_for(std::chrono::seconds(1));
  LOG(INFO) << "starting bench";
  auto start = std::chrono::system_clock::now();

  // SeqPut.  Start with "0->0", and each callback will launch the next pair.
  const std::string kZeroStr = "0";
  const int status =
      redisAsyncCommand(write_context, &SeqPutCallback,
                        /*privdata=*/NULL, "MEMBER.PUT %b %b %b",
                        kZeroStr.data(), kZeroStr.size(), kZeroStr.data(),
                        kZeroStr.size(), client_id.data(), client_id.size());
  CHECK(status == REDIS_OK);

  LOG(INFO) << "start loop";
  aeMain(loop);
  LOG(INFO) << "end loop";

  auto end = std::chrono::system_clock::now();
  CHECK(num_completed == N)
      << "num_completed " << num_completed << " vs N " << N;
  LOG(INFO) << "ending bench";

  const int64_t latency_us =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  LOG(INFO) << "throughput " << N * 1e6 / latency_us
            << " writes/s, total duration (ms) " << latency_us / 1e3
            << ", num_ops " << N << ", num_nodes " << num_chain_nodes;

  return 0;
}
