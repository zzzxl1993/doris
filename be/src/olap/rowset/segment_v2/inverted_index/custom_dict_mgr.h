// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <CLucene.h>
#include <sys/stat.h>

#include <chrono>
#include <ctime>
#include <filesystem>
#include <memory>
#include <mutex>
#include <utility>

#include "util/countdown_latch.h"
#include "util/thread.h"

namespace doris {

class CustomDictMgr {
public:
    CustomDictMgr(std::string stop_words_path, std::string user_dict_path);
    ~CustomDictMgr() = default;

    void init();
    void stop();

    lucene::analysis::WordsPtr getStopWords(const std::string& name);

private:
    struct Data {
        std::filesystem::file_time_type _modification_time;
        lucene::analysis::WordsPtr _words;
    };
    using DataPtr = std::shared_ptr<Data>;

    void userWordsWoker();
    bool loadStopWords(const std::string& name);

private:
    static constexpr int32_t TIME_SECOND = 60;

    static constexpr int32_t FILE_COUNT = 100;
    static constexpr int32_t FILE_LINE_COUNT = 100;
    static constexpr int32_t FILE_LINE_SIZE = 20;
    
private:
    std::string _stop_words_path;
    std::string _user_dict_path;
    std::map<std::string, DataPtr> _stop_words;
    std::map<std::string, DataPtr> _user_dict_words;

    std::shared_mutex _mutex;
    CountDownLatch _stop_background_threads_latch;
    scoped_refptr<Thread> _thread;
};

} // namespace doris