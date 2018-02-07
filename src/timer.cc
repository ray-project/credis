#include <cmath>
#include <string>

#include <sys/time.h>
#include <time.h>

#include "glog/logging.h"

#include "timer.h"

Timer Timer::Merge(Timer& timer1, Timer& timer2) {
  Timer t;
  auto& lat = t.latency_micros();
  auto& lat1 = timer1.latency_micros();
  auto& lat2 = timer2.latency_micros();
  lat.reserve(lat1.size() + lat2.size());
  lat.insert(lat.end(), lat1.begin(), lat1.end());
  lat.insert(lat.end(), lat2.begin(), lat2.end());
  return t;
}

double Timer::NowMicrosecs() const {
  struct timeval time;
  gettimeofday(&time, NULL);
  return (double) time.tv_sec * 1e6 + (double) time.tv_usec;
}

void Timer::ExpectOps(int N) {
  begin_timestamps_.reserve(N);
  latency_micros_.reserve(N);
}

double Timer::TimeOpBegin() {
  const double now = NowMicrosecs();
  CHECK(latency_micros_.size() == begin_timestamps_.size())
      << " sizes " << latency_micros_.size() << " " << begin_timestamps_.size();
  begin_timestamps_.push_back(now);
  return now;
}

void Timer::TimeOpEnd(int num_completed) {
  const double now = NowMicrosecs();
  CHECK(latency_micros_.size() == num_completed - 1);
  CHECK(begin_timestamps_.size() == num_completed)
      << begin_timestamps_.size() << " " << num_completed;
  latency_micros_.push_back(now - begin_timestamps_.back());
}

void Timer::Stats(double* mean, double* std) const {
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

std::string Timer::ReportStats(const std::string& name) const {
  double mean = 0, std = 0;
  Stats(&mean, &std);

  std::string msg = name;
  msg += " mean ";
  msg += std::to_string(mean);
  msg += " std ";
  msg += std::to_string(std);
  msg += " num ";
  msg += std::to_string(latency_micros_.size());
  return msg;
}

std::vector<double>& Timer::begin_timestamps() { return begin_timestamps_; }
std::vector<double>& Timer::latency_micros() { return latency_micros_; }
