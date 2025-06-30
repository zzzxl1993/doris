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

#include "disjunction_query.h"

#include "olap/rowset/segment_v2/inverted_index/query/query_helper.h"

namespace doris::segment_v2 {

DisjunctionQuery::DisjunctionQuery(SearcherPtr searcher, IndexQueryContextPtr context)
        : _searcher(std::move(searcher)), _context(std::move(context)) {}

DisjunctionQuery::DisjunctionQuery(SearcherPtr searcher, IndexQueryContextPtr context,
                                   bool is_similarity)
        : DisjunctionQuery(std::move(searcher), std::move(context)) {
    _is_similarity = is_similarity;
}

void DisjunctionQuery::add(const InvertedIndexQueryInfo& query_info) {
    if (query_info.terms.empty()) {
        _CLTHROWA(CL_ERR_IllegalArgument, "DisjunctionQuery::add: terms empty");
    }

    _field_name = query_info.field_name;

    for (const auto& term : query_info.terms) {
        auto iter = TermIterator::create(_context->io_ctx, _searcher->getReader(),
                                         query_info.field_name, term);
        _iterators.emplace_back(iter);
    }

    if (_is_similarity && _context->collection_similarity) {
        for (const auto& iter : _iterators) {
            auto similarity = std::make_unique<BM25Similarity>();
            similarity->for_one_term(_context, _field_name, iter->term());
            _similarities.emplace_back(std::move(similarity));
        }
    }
}

void DisjunctionQuery::pre_search(const InvertedIndexQueryInfo& query_info) {
    if (query_info.terms.empty()) {
        return;
    }

    QueryHelper::query_statistics(_context, _searcher, query_info.field_name, query_info.terms);
}

void DisjunctionQuery::search(roaring::Roaring& roaring) {
    auto func = [this, &roaring](size_t i, const TermIterPtr& iter, bool first) {
        DocRange doc_range;
        roaring::Roaring result;
        while (iter->read_range(&doc_range)) {
            if (doc_range.type_ == DocRangeType::kMany) {
                result.addMany(doc_range.doc_many_size_, doc_range.doc_many->data());

                if (_is_similarity && _context->collection_similarity) {
                    QueryHelper::collect_many(_context, _similarities[i], doc_range, roaring,
                                              first);
                }
            } else {
                result.addRange(doc_range.doc_range.first, doc_range.doc_range.second);

                if (_is_similarity && _context->collection_similarity) {
                    QueryHelper::collect_range(_context, _similarities[i], doc_range, roaring,
                                               first);
                }
            }
        }

        if (first) {
            roaring.swap(result);
        } else {
            roaring |= result;
        }
    };
    for (size_t i = 0; i < _iterators.size(); i++) {
        func(i, _iterators[i], i == 0);
    }
}

} // namespace doris::segment_v2