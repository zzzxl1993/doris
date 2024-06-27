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

#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

Status FunctionMultiMatch::execute_impl(FunctionContext* /*context*/, Block& block,
                                        const ColumnNumbers& arguments, size_t result,
                                        size_t /*input_rows_count*/) const {
    // auto column_res = ColumnUInt8::create(1);
    // auto& column_data = column_res->get_data();
    // column_data[0] = false;
    // block.replace_by_position(result, std::move(column_res));
    // return Status::OK();

    return Status::RuntimeError("only inverted index queries are supported");
}

void register_function_multi_match(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMultiMatch>();
}

} // namespace doris::vectorized
