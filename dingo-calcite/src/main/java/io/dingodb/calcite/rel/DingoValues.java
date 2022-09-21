/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.calcite.rel;

import io.dingodb.calcite.visitor.DingoRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import javax.annotation.Nonnull;

public class DingoValues extends LogicalDingoValues implements DingoRel {
    public DingoValues(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelDataType rowType,
        List<Object[]> tuples
    ) {
        super(cluster, traits, rowType, tuples);
    }

    @Override
    public <T> T accept(@Nonnull DingoRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return getTuples().size();
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(@Nonnull RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }
}
