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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.LeafPlan;
import org.apache.doris.nereids.trees.plans.PlanType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Abstract class for all logical plan that have no child.
 */
public abstract class LogicalLeaf extends AbstractLogicalPlan implements LeafPlan, OutputSavePoint {

    protected LogicalLeaf(PlanType nodeType, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties) {
        super(nodeType, groupExpression, logicalProperties, ImmutableList.of());
    }

    protected LogicalLeaf(PlanType nodeType, Optional<GroupExpression> groupExpression,
            Supplier<LogicalProperties> logicalPropertiesSupplier, boolean useZeroId) {
        super(nodeType, groupExpression, logicalPropertiesSupplier, ImmutableList.of(), useZeroId);
    }

    public abstract List<Slot> computeOutput();
}
