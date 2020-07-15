/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.traversal.optimize;

import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public final class HugeGraphStepStrategy
             extends AbstractTraversalStrategy<ProviderOptimizationStrategy>
             implements ProviderOptimizationStrategy {

    private static final long serialVersionUID = -2952498905649139719L;

    private static final HugeGraphStepStrategy INSTANCE;

    static {
        INSTANCE = new HugeGraphStepStrategy();
    }

    private HugeGraphStepStrategy() {
        // pass
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void apply(Traversal.Admin<?, ?> traversal) {
        //校验HasStep
        TraversalUtil.convAllHasSteps(traversal);

        // Extract conditions in GraphStep
        List<GraphStep> steps = TraversalHelper.getStepsOfClass(
                                GraphStep.class, traversal);
        for (GraphStep originStep : steps) {
            //转换成 HugeGraphStep
            HugeGraphStep<?, ?> newStep = new HugeGraphStep<>(originStep);
            //更新Graph traversal中的step
            TraversalHelper.replaceStep(originStep, newStep, traversal);
            //转换或剔除hasStep
            TraversalUtil.extractHasContainer(newStep, traversal);

            // TODO: support order-by optimize
            // TraversalUtil.extractOrder(newStep, traversal);

            //更新Range查询中的策略
            TraversalUtil.extractRange(newStep, traversal, false);
            //更新count上级返回结果上限
            TraversalUtil.extractCount(newStep, traversal);
        }
    }

    public static HugeGraphStepStrategy instance() {
        return INSTANCE;
    }
}
