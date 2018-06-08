#ifndef CREDIS_TIMER_
#define CREDIS_TIMER_

#include <fstream>
#include <iostream>
#include <vector>

// For sequential queries only (i.e., launch next after current finishes).
class Timer {
 public:
  static Timer Merge(Timer& timer1, Timer& timer2);
  double NowMicrosecs() const;

  void ExpectOps(int N);

  double TimeOpBegin();
  void TimeOpEnd(int num_completed);

  void Stats(double* mean, double* std) const;
  std::string ReportStats(const std::string& name) const;
  double Min() const;
  double Max() const;

  void DropFirst(int n);

  void WriteToFile(const std::string& path) const;
  void AppendToFile(const std::string& path) const;

  std::vector<double>& begin_timestamps();
  std::vector<double>& latency_micros();

 private:
  void ToFile(const std::string& path, bool is_append) const;

  std::vector<double> begin_timestamps_;
  std::vector<double> latency_micros_;
};

#endif  // CREDIS_TIMER_
