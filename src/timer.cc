#include "timer.h"

#include <cmath>
#include <string>

#include <sys/time.h>
#include <time.h>

#include "glog/logging.h"

void Timer::ToFile(const std::string& path, bool is_append) const {
  CHECK(begin_timestamps_.size() == latency_micros_.size())
      << begin_timestamps_.size() << " " << latency_micros_.size();
  std::ios_base::openmode mode = std::ios_base::out;
  if (is_append) mode = std::ios_base::app;
  std::ofstream ofs(path, mode);

  if (!is_append) ofs << "begin_timestamp_us,latency_us" << std::endl;

  for (int i = 0; i < begin_timestamps_.size(); ++i) {
    ofs << static_cast<int64_t>(begin_timestamps_[i]) << ","
        << latency_micros_[i] << std::endl;
  }
}

Timer Timer::Merge(Timer& timer1, Timer& timer2) {
  Timer t;
  auto& lat = t.latency_micros();
  auto& lat1 = timer1.latency_micros();
  auto& lat2 = timer2.latency_micros();
  lat.reserve(lat1.size() + lat2.size());
  lat.insert(lat.end(), lat1.begin(), lat1.end());
  lat.insert(lat.end(), lat2.begin(), lat2.end());

  auto& timestamps = t.begin_timestamps();
  auto& timestamps1 = timer1.begin_timestamps();
  auto& timestamps2 = timer2.begin_timestamps();
  timestamps.reserve(timestamps1.size() + timestamps2.size());
  timestamps.insert(timestamps.end(), timestamps1.begin(), timestamps1.end());
  timestamps.insert(timestamps.end(), timestamps2.begin(), timestamps2.end());
  return t;
}

double Timer::NowMicrosecs() const {
  struct timeval time;
  gettimeofday(&time, NULL);
  return (double)time.tv_sec * 1e6 + (double)time.tv_usec;
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

void Timer::DropFirst(int n) {
  begin_timestamps_.erase(begin_timestamps_.begin(),
                          begin_timestamps_.begin() + n);
  latency_micros_.erase(latency_micros_.begin(), latency_micros_.begin() + n);
}

void Timer::WriteToFile(const std::string& path) const {
  ToFile(path, /*is_append=*/false);
}
void Timer::AppendToFile(const std::string& path) const {
  ToFile(path, /*is_append=*/true);
}

std::vector<double>& Timer::begin_timestamps() { return begin_timestamps_; }
std::vector<double>& Timer::latency_micros() { return latency_micros_; }
