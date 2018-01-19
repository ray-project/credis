#ifndef CREDIS_TIMER_
#define CREDIS_TIMER_

#include <vector>

#include <sys/time.h>
#include <time.h>

#include "glog/logging.h"

// For sequential queries only (i.e., launch next after current finishes).
class Timer {
 public:
  void ExpectOps(int N) {
    begin_timestamps_.reserve(N);
    latency_micros_.reserve(N);
  }
  double NowMicrosecs() {
    struct timeval time;
    gettimeofday(&time, NULL);
    return (double) time.tv_sec * 1e6 + (double) time.tv_usec;
  }
  void TimeOpBegin() {
    const double now = NowMicrosecs();
    CHECK(latency_micros_.size() == begin_timestamps_.size())
        << " sizes " << latency_micros_.size() << " "
        << begin_timestamps_.size();
    begin_timestamps_.push_back(now);
  }
  void TimeOpEnd(int num_completed) {
    const double now = NowMicrosecs();
    CHECK(latency_micros_.size() == num_completed - 1);
    CHECK(begin_timestamps_.size() == num_completed);
    latency_micros_.push_back(now - begin_timestamps_.back());
  }
  void Stats(double* mean, double* std) {
    CHECK(!latency_micros_.empty());
    double sum = 0;
    for (const double x : latency_micros_) sum += x;
    *mean = sum / latency_micros_.size();

    sum = 0;
    for (const double x : latency_micros_) sum += (x - *mean) * (x - *mean);
    *std = std::sqrt(sum / latency_micros_.size());
  }

  const std::vector<double>& latency_micros() { return latency_micros_; }

 private:
  std::vector<double> begin_timestamps_;
  std::vector<double> latency_micros_;
};

#endif  // CREDIS_TIMER_
