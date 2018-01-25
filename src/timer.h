#ifndef CREDIS_TIMER_
#define CREDIS_TIMER_

#include <cmath>
#include <vector>

#include <sys/time.h>
#include <time.h>

#include "glog/logging.h"

// For sequential queries only (i.e., launch next after current finishes).
class Timer {
 public:
  static Timer Merge(Timer& timer1, Timer& timer2) {
    Timer t;
    auto& lat = t.latency_micros();
    auto& lat1 = timer1.latency_micros();
    auto& lat2 = timer2.latency_micros();
    lat.reserve(lat1.size() + lat2.size());
    lat.insert(lat.end(), lat1.begin(), lat1.end());
    lat.insert(lat.end(), lat2.begin(), lat2.end());
    return t;
  }

  void ExpectOps(int N) {
    begin_timestamps_.reserve(N);
    latency_micros_.reserve(N);
  }
  double NowMicrosecs() {
    struct timeval time;
    gettimeofday(&time, NULL);
    return (double) time.tv_sec * 1e6 + (double) time.tv_usec;
  }
  double TimeOpBegin() {
    const double now = NowMicrosecs();
    CHECK(latency_micros_.size() == begin_timestamps_.size())
        << " sizes " << latency_micros_.size() << " "
        << begin_timestamps_.size();
    begin_timestamps_.push_back(now);
    return now;
  }
  void TimeOpEnd(int num_completed) {
    const double now = NowMicrosecs();
    CHECK(latency_micros_.size() == num_completed - 1);
    CHECK(begin_timestamps_.size() == num_completed)
        << begin_timestamps_.size() << " " << num_completed;
    latency_micros_.push_back(now - begin_timestamps_.back());
  }
  void Stats(double* mean, double* std) {
    if (latency_micros_.empty()) {
      *mean = 0;
      *std = 0;
      return;
    }
    double sum = 0;
    for (const double x : latency_micros_) sum += x;
    *mean = sum / latency_micros_.size();

    sum = 0;
    for (const double x : latency_micros_) sum += (x - *mean) * (x - *mean);
    *std = std::sqrt(sum / latency_micros_.size());
  }

  std::vector<double>& begin_timestamps() { return begin_timestamps_; }
  std::vector<double>& latency_micros() { return latency_micros_; }

 private:
  std::vector<double> begin_timestamps_;
  std::vector<double> latency_micros_;
};

#endif  // CREDIS_TIMER_
