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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.trees.expressions.Expression;

import java.util.List;

/**
 * some function PropagateNullable when args are datev2 or datetimev2 or time like
 * and AlwaysNullable when other type parameters
 */
public interface PropagateNullableOnDateOrTimeLikeV2Args extends PropagateNullable, AlwaysNullable {
    @Override
    default boolean nullable() {
        if (children().stream().anyMatch(e -> e.getDataType().isDateV2LikeType() || e.getDataType().isTimeType())) {
            return PropagateNullable.super.nullable();
        } else {
            return AlwaysNullable.super.nullable();
        }
    }

    List<Expression> children();
}
