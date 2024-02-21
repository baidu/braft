// Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <assert.h>

#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#ifdef NDEBUG
#define TEST_SYNC_POINT(x)
#define TEST_IDX_SYNC_POINT(x, index)
#define TEST_SYNC_POINT_CALLBACK(x, y)
#define INIT_SYNC_POINT_SINGLETONS()
#else

namespace braft {

// This class provides facility to reproduce race conditions deterministically
// in unit tests.
// Developer could specify sync points in the codebase via TEST_SYNC_POINT.
// Each sync point represents a position in the execution stream of a thread.
// In the unit test, 'Happens After' relationship among sync points could be
// setup via SyncPoint::LoadDependency, to reproduce a desired interleave of
// threads execution.

class SyncPoint {
 public:
  static SyncPoint* GetInstance();

  SyncPoint(const SyncPoint&) = delete;
  SyncPoint& operator=(const SyncPoint&) = delete;
  ~SyncPoint();

  struct SyncPointPair {
    std::string predecessor;
    std::string successor;
  };

  // call once at the beginning of a test to setup the dependency between
  // sync points. Specifically, execution will not be allowed to proceed past
  // each successor until execution has reached the corresponding predecessor,
  // in any thread.
  void LoadDependency(const std::vector<SyncPointPair>& dependencies);

  // call once at the beginning of a test to setup the dependency between
  // sync points and setup markers indicating the successor is only enabled
  // when it is processed on the same thread as the predecessor.
  // When adding a marker, it implicitly adds a dependency for the marker pair.
  void LoadDependencyAndMarkers(const std::vector<SyncPointPair>& dependencies,
                                const std::vector<SyncPointPair>& markers);

  // The argument to the callback is passed through from
  // TEST_SYNC_POINT_CALLBACK(); nullptr if TEST_SYNC_POINT or
  // TEST_IDX_SYNC_POINT was used.
  void SetCallBack(const std::string& point,
                   const std::function<void(void*)>& callback);

  // Clear callback function by point
  void ClearCallBack(const std::string& point);

  // Clear all call back functions.
  void ClearAllCallBacks();

  // enable sync point processing (disabled on startup)
  void EnableProcessing();

  // disable sync point processing
  void DisableProcessing();

  // remove the execution trace of all sync points
  void ClearTrace();

  // triggered by TEST_SYNC_POINT, blocking execution until all predecessors
  // are executed.
  // And/or call registered callback function, with argument `cb_arg`
  void Process(const std::string& point, void* cb_arg = nullptr);

  // template gets length of const string at compile time,
  //  avoiding strlen() at runtime
  template <size_t kLen>
  void Process(const char (&point)[kLen], void* cb_arg = nullptr) {
    static_assert(kLen > 0, "Must not be empty");
    assert(point[kLen - 1] == '\0');
    Process(std::string(point, kLen - 1), cb_arg);
  }

  // TODO: it might be useful to provide a function that blocks until all
  // sync points are cleared.

  // We want this to be public so we can
  // subclass the implementation
  struct Data;

 private:
  // Singleton
  SyncPoint();
  Data* impl_;
};

// Sets up sync points to mock direct IO instead of actually issuing direct IO
// to the file system.
void SetupSyncPointsToMockDirectIO();
}  // namespace braft

// Use TEST_SYNC_POINT to specify sync points inside code base.
// Sync points can have happens-after dependency on other sync points,
// configured at runtime via SyncPoint::LoadDependency. This could be
// utilized to re-produce race conditions between threads.
// See TransactionLogIteratorRace in db_test.cc for an example use case.
// TEST_SYNC_POINT is no op in release build.
#define TEST_SYNC_POINT(x) \
  braft::SyncPoint::GetInstance()->Process(x)
#define TEST_IDX_SYNC_POINT(x, index)                      \
  braft::SyncPoint::GetInstance()->Process(x + \
                                                       std::to_string(index))
#define TEST_SYNC_POINT_CALLBACK(x, y) \
  braft::SyncPoint::GetInstance()->Process(x, y)
#define INIT_SYNC_POINT_SINGLETONS() \
  (void)braft::SyncPoint::GetInstance();
#endif  // NDEBUG

// Callback sync point for any read IO errors that should be ignored by
// the fault injection framework
// Disable in release mode
#ifdef NDEBUG
#define IGNORE_STATUS_IF_ERROR(_status_)
#else
#define IGNORE_STATUS_IF_ERROR(_status_)            \
  {                                                 \
    if (!_status_.ok()) {                           \
      TEST_SYNC_POINT("FaultInjectionIgnoreError"); \
    }                                               \
  }
#endif  // NDEBUG
