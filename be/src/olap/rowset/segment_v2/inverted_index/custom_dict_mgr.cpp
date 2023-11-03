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

#include "custom_dict_mgr.h"

#include <memory>

#include "CLucene/analysis/AnalysisHeader.h"

namespace doris {

CustomDictMgr::CustomDictMgr(std::string stop_words_path, std::string user_dict_path)
        : _stop_words_path(std::move(stop_words_path)),
          _user_dict_path(std::move(user_dict_path)),
          _stop_background_threads_latch(1) {}

void CustomDictMgr::init() {
    CHECK(Thread::create(
                  "CustomDictMgr", "userWordsWoker", [this]() { userWordsWoker(); }, &_thread)
                  .ok());
}

void CustomDictMgr::stop() {
    _stop_background_threads_latch.count_down();
    _thread->join();
}

lucene::analysis::WordsPtr CustomDictMgr::getStopWords(const std::string& name) {
    if (name.empty()) {
        return nullptr;
    }

    {
        std::shared_lock<std::shared_mutex> rlock(_mutex);
        auto iter = _stop_words.find(name);
        if (iter != _stop_words.end()) {
            return iter->second->_words;
        }
    }

    std::lock_guard<std::shared_mutex> wlock(_mutex);
    auto iter = _stop_words.find(name);
    if (iter != _stop_words.end()) {
        return iter->second->_words;
    }
    if (!loadStopWords(name)) {
        return nullptr;
    }
    return _stop_words[name]->_words;
}

void CustomDictMgr::userWordsWoker() {
    auto func = [this](const std::string& full_path, DataPtr& data) {
        if (std::filesystem::exists(full_path)) {
            auto new_modification_time = std::filesystem::last_write_time(full_path);
            if (new_modification_time != data->_modification_time) {
                auto words_ptr = std::make_shared<lucene::analysis::Words>();
                {
                    std::ifstream file(full_path);
                    std::string line;
                    for (int32_t i = 0; i < FILE_LINE_COUNT; i++) {
                        if (!std::getline(file, line)) {
                            break;
                        }
                        if (line.size() > FILE_LINE_SIZE) {
                            continue;
                        }
                        words_ptr->insert(line);
                    }
                }
                
                LOG(ERROR) << "stop words file update: " << full_path << ", size: " << _stop_words.size();
                std::lock_guard<std::shared_mutex> wlock(_mutex);
                data->_modification_time = new_modification_time;
                data->_words = words_ptr;
            }
        }
    };

    do {
        for (auto& iter : _stop_words) {
            std::string full_path = _stop_words_path + "/" + iter.first;
            func(full_path, iter.second);
        }

        for (auto& iter : _user_dict_words) {
            std::string full_path = _user_dict_path + "/" + iter.first;
            func(full_path, iter.second);
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(TIME_SECOND)));
}

bool CustomDictMgr::loadStopWords(const std::string& name) {
    if (_stop_words.size() >= FILE_COUNT) {
        return false;
    }

    std::string full_path = _stop_words_path + "/" + name;
    if (!std::filesystem::exists(full_path)) {
        return false;
    }

    auto modification_time = std::filesystem::last_write_time(full_path);
    auto words_ptr = std::make_shared<lucene::analysis::Words>();
    {
        std::ifstream file(full_path);
        std::string line;
        for (int32_t i = 0; i < FILE_LINE_COUNT; i++) {
            if (!std::getline(file, line)) {
                break;
            }
            if (line.size() > FILE_LINE_SIZE) {
                continue;
            }
            words_ptr->insert(line);
        }
    }

    LOG(ERROR) << "stop words file add: " << full_path << ", size: " << _stop_words.size();
    DataPtr data_ptr = std::make_shared<Data>();
    data_ptr->_modification_time = modification_time;
    data_ptr->_words = words_ptr;
    _stop_words[name] = data_ptr;

    return true;
}

} // namespace doris