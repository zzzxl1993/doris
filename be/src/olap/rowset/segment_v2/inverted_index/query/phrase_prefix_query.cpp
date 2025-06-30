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

#include "phrase_prefix_query.h"

#include "olap/rowset/segment_v2/inverted_index/query/query_helper.h"

namespace doris::segment_v2 {

PhrasePrefixQuery::PhrasePrefixQuery(SearcherPtr searcher, IndexQueryContextPtr context)
        : _searcher(std::move(searcher)),
          _context(std::move(context)),
          _phrase_query(_searcher, _context),
          _prefix_query(_searcher, _context) {
    _max_expansions = _context->runtime_state->query_options().inverted_index_max_expansions;
}

void PhrasePrefixQuery::add(const InvertedIndexQueryInfo& query_info) {
    if (query_info.terms.empty()) {
        _CLTHROWA(CL_ERR_IllegalArgument, "PhrasePrefixQuery::add: terms empty");
    }

    _term_size = query_info.terms.size();
    std::vector<std::vector<std::wstring>> terms(query_info.terms.size());
    for (size_t i = 0; i < query_info.terms.size(); i++) {
        if (i < query_info.terms.size() - 1) {
            std::wstring ws = StringUtil::string_to_wstring(query_info.terms[i]);
            terms[i].emplace_back(ws);
        } else {
            _prefix_query.get_prefix_terms(_searcher->getReader(), query_info.field_name,
                                           query_info.terms[i], terms[i], _max_expansions);
            if (terms[i].empty()) {
                std::wstring ws = StringUtil::string_to_wstring(query_info.terms[i]);
                terms[i].emplace_back(ws);
            }
        }
    }

    if (_term_size == 1) {
        _prefix_query.add(query_info.field_name, terms[0]);
    } else {
        _phrase_query.add(query_info.field_name, terms);
    }
}

void PhrasePrefixQuery::pre_search(const InvertedIndexQueryInfo& query_info) {
    if (query_info.terms.size() < 2) {
        return;
    }

    auto span = std::span<const std::string>(query_info.terms.begin(), query_info.terms.end() - 1);
    QueryHelper::query_statistics(_context, _searcher, query_info.field_name, span);
}

void PhrasePrefixQuery::search(roaring::Roaring& roaring) {
    if (_term_size == 1) {
        _prefix_query.search(roaring);
    } else {
        _phrase_query.search(roaring);
    }
}

} // namespace doris::segment_v2