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

#include "vec/functions/function_multi_match.h"

#include <gen_cpp/PaloBrokerService_types.h>

#include <boost/algorithm/string.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "io/fs/file_reader.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/inverted_index/query/phrase_prefix_query.h"
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "vec/columns/column.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/varray_literal.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

void parse_string_to_set(std::set<std::string>& res, const std::string& src,
                         const std::string& pattern) {
    std::vector<std::string> tokens;
    boost::split(tokens, src, boost::is_any_of(pattern), boost::token_compress_on);

    for (std::string& token : tokens) {
        boost::trim(token);
        if (!token.empty()) {
            res.insert(token);
        }
    }
}

Status FunctionMultiMatch::execute_impl(FunctionContext* /*context*/, Block& block,
                                        const ColumnNumbers& arguments, size_t result,
                                        size_t /*input_rows_count*/) const {
    return Status::RuntimeError("only inverted index queries are supported");
}

Status FunctionMultiMatch::eval_inverted_index(const VExpr* expr,
                                               segment_v2::SegmentIterator* segment_iterator,
                                               std::shared_ptr<roaring::Roaring>& bitmap) {
    if (expr->children().size() != 4) {
        return Status::RuntimeError("--- 1 ---");
    }

    auto expr_0 = expr->get_child(0);
    auto expr_1 = expr->get_child(1);
    auto expr_2 = expr->get_child(2);
    auto expr_3 = expr->get_child(3);
    if (!(expr_0 && expr_0->is_slot_ref()) ||
        !(expr_1 && expr_1->is_literal() && is_string(expr_1->data_type())) ||
        !(expr_2 && expr_2->is_literal() && is_string(expr_2->data_type())) ||
        !(expr_3 && expr_3->is_literal() && is_string(expr_3->data_type()))) {
        return Status::RuntimeError("--- 2 ---");
    }

    auto confirm_expr = std::static_pointer_cast<VSlotRef>(expr_0);
    auto addition_expr = std::static_pointer_cast<VLiteral>(expr_1);
    auto query_type_expr2 = std::static_pointer_cast<VLiteral>(expr_2);
    auto query_string_expr3 = std::static_pointer_cast<VLiteral>(expr_3);

    std::set<std::string> column_names;
    column_names.insert(confirm_expr->expr_name());
    parse_string_to_set(column_names, addition_expr->value(), ",");
    std::string query_type_str = query_type_expr2->value();
    std::string query_string = query_string_expr3->value();

    std::vector<ColumnId> columns_ids;
    for (const auto& column_name : column_names) {
        auto cid = segment_iterator->_opts.tablet_schema->field_index(column_name);
        if (cid < 0) {
            return Status::RuntimeError("--- 3 ---");
        }
        columns_ids.emplace_back(cid);
    }

    // query type
    InvertedIndexQueryType query_type;
    if (query_type_str == "match_phrase_prefix") {
        query_type = InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY;
    } else {
        return Status::RuntimeError("--- 4 ---");
    }

    // cache key
    roaring::Roaring cids_str;
    cids_str.addMany(columns_ids.size(), columns_ids.data());
    cids_str.runOptimize();
    std::string column_name_buf(cids_str.getSizeInBytes(), 0);
    cids_str.write(column_name_buf.data());

    io::Path index_path = segment_iterator->_segment->file_reader()->path();

    InvertedIndexQueryCache::CacheKey cache_key;
    cache_key.index_path = index_path.parent_path() / index_path.stem();
    cache_key.column_name = column_name_buf;
    cache_key.query_type = query_type;
    cache_key.value = query_string;

    // query cache
    auto* cache = InvertedIndexQueryCache::instance();
    InvertedIndexQueryCacheHandle cache_handler;
    if (cache->lookup(cache_key, &cache_handler)) {
        bitmap = cache_handler.get_bitmap();
        return Status::OK();
    }

    // search
    bool first = true;
    const auto& tablet_schema = segment_iterator->_opts.tablet_schema;
    for (const auto& column_name : column_names) {
        auto cid = tablet_schema->field_index(column_name);

        const auto& inverted_index_iterator = segment_iterator->_inverted_index_iterators[cid];
        if (!inverted_index_iterator) {
            return Status::RuntimeError("--- 5 ---");
        }
        const auto& inverted_index_reader = inverted_index_iterator->_reader;
        if (!inverted_index_iterator) {
            return Status::RuntimeError("--- 6 ---");
        }

        // tokenize
        std::vector<std::string> terms;
        InvertedIndexCtxSPtr inverted_index_ctx = std::make_shared<InvertedIndexCtx>();
        inverted_index_ctx->parser_type = InvertedIndexParserType::PARSER_UNICODE;
        auto analyzer = inverted_index_reader->create_analyzer(inverted_index_ctx.get());
        analyzer->set_lowercase(true);
        auto reader = inverted_index_reader->create_reader(inverted_index_ctx.get(), query_string);
        inverted_index_reader->get_analyse_result(terms, reader.get(), analyzer.get(), column_name,
                                                  query_type);

        // phrase prefix query
        FulltextIndexSearcherPtr* searcher_ptr = nullptr;
        InvertedIndexCacheHandle inverted_index_cache_handle;
        RETURN_IF_ERROR(inverted_index_reader->handle_searcher_cache(
                &inverted_index_cache_handle, inverted_index_iterator->_stats));
        auto searcher_variant = inverted_index_cache_handle.get_index_searcher();
        searcher_ptr = std::get_if<FulltextIndexSearcherPtr>(&searcher_variant);
        if (searcher_ptr != nullptr) {
            roaring::Roaring result;
            TQueryOptions queryOptions = inverted_index_iterator->_runtime_state->query_options();
            segment_v2::PhrasePrefixQuery query(*searcher_ptr, queryOptions);
            query.add(StringUtil::string_to_wstring(column_name), terms);
            query.search(result);
            if (first) {
                (*bitmap).swap(result);
                first = false;
            } else {
                (*bitmap) |= result;
            }
        }
    }

    bitmap->runOptimize();
    cache->insert(cache_key, bitmap, &cache_handler);

    return Status::OK();
}

void register_function_multi_match(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMultiMatch>();
}

} // namespace doris::vectorized
