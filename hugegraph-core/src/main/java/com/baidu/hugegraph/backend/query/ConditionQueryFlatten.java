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

package com.baidu.hugegraph.backend.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.NumericUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Flatten将Condition展开
 * 1.List参数类型Relation，展开成多个Relation的（EQ，NEQ）类型
 * 2.And 、Or类型的condition，展开成Relations，堆叠多有Relation
 * 3.
 */
public final class ConditionQueryFlatten {

    private static final Set<HugeKeys> SPECIAL_KEYS = ImmutableSet.of(
            HugeKeys.LABEL
    );

    /**
     * 将ConditionQuery展开，最后生成List<ConditionQuery>，每个ConditionQuery互为or的逻辑关系
     * 展开逻辑：
     * 1.将复合类型参数展开，针对IN，NOT_IN，CONTAIN_ANY，参数为List<>等
     * 2.将And与Or类型的Condition展开，得到堆叠的Set<Relations>，彼此关系为Or的逻辑条件列表
     * 3.优化2的结果Set<Relations>
     * 最后将1与2结果合并，得到List<ConditionQuery>，此次为Or
     * @param query
     * @return
     */
    public static List<ConditionQuery> flatten(ConditionQuery query) {
        //已经是扁平的;
        //query.isFlattened() 所有的condition都是Relation，And与Or可以向下展开
        //query.mayHasDupKeys(SPECIAL_KEYS) 同一个字段没有多个规则的情况 (a>1 and a>10)
        if (query.isFlattened() && !query.mayHasDupKeys(SPECIAL_KEYS)) {
            return Arrays.asList(query);
        }

        List<ConditionQuery> queries = new ArrayList<>();

        // Flatten IN/NOT_IN if needed
        //展开复合参数
        Set<Condition> conditions = InsertionOrderUtil.newSet();

        for (Condition condition : query.conditions()) {
            //展开复合参数
            Condition cond = flattenIn(condition);
            if (cond == null) {
                // Process 'XX in []'
                return ImmutableList.of();
            }
            conditions.add(cond);
        }
        query = query.copy();
        query.resetConditions(conditions);

        //对And与Or复合条件进行展开，得到堆叠的Set<Relations>
        // Flatten OR if needed
        Set<Relations> results = null;
        for (Condition condition : query.conditions()) {
            if (results == null) {
                results = flattenAndOr(condition);
            } else {
                results = and(results, flattenAndOr(condition));
            }
        }

        // Optimize useless condition
        assert results != null;
        //Set<Relations> relation之间是or的逻辑关系
        for (Relations relations : results) {
            relations = optimizeRelations(relations);
            /*
             * Relations may be null after optimize, for example:
             * age > 10 and age == 9
             */
            if (relations == null) {
                continue;
            }
            //合并优化后的Set<Relations>与 Flatten IN/NOT_IN 的ConditionQuery
            ConditionQuery cq = newQueryFromRelations(query, relations);
            if (cq != null) {
                //每一个ConditionQuery之间是or的关系
                queries.add(cq);
            }
        }

        return queries;
    }

    /**
     * 将condition中参数为复合结构的，如List进行展开为，多个条件or拼接
     * @param condition
     * @return
     */
    private static Condition flattenIn(Condition condition) {
        switch (condition.type()) {
            case RELATION:
                Relation relation = (Relation) condition;
                //将多个输入参数 展开成多个 条件计算，(in (1,2,3))->(=1 or=2 or=3)
                switch (relation.relation()) {
                    case IN:
                        // Flatten IN if needed
                        return convIn2Or(relation);
                    case NOT_IN:
                        // Flatten NOT_IN if needed
                        return convNotin2And(relation);
                    case TEXT_CONTAINS_ANY:
                        // Flatten TEXT_CONTAINS_ANY if needed
                        return convTextContainsAny2Or(relation);
                    default:
                        return condition;
                }
            case AND:
                Condition.And and = (Condition.And) condition;
                return new Condition.And(flattenIn(and.left()),
                                         flattenIn(and.right()));
            case OR:
                Condition.Or or = (Condition.Or) condition;
                return new Condition.Or(flattenIn(or.left()),
                                        flattenIn(or.right()));
            default:
                throw new AssertionError(String.format(
                          "Wrong condition type: '%s'", condition.type()));
        }
    }

    /**
     * 将Relation类型为IN的表达式 多个输入参数，进行展开。
     * 以or（条件关系）进行拼接,EQ类型的Relation
     * RelationType.IN ==>Condition.or(1) or(2) or(3) ...
     * @param relation relation type of in
     * @return
     */
    private static Condition convIn2Or(Relation relation) {
        assert relation.relation() == Condition.RelationType.IN;
        Object key = relation.key();
        @SuppressWarnings("unchecked")
        List<Object>  values = (List<Object>) relation.value();
        Condition cond, conds = null;
        for (Object value : values) {
            if (key instanceof HugeKeys) {
                cond = Condition.eq((HugeKeys) key, value);
            } else {
                cond = Condition.eq((Id) key, value);
            }
            conds = conds == null ? cond : Condition.or(conds, cond);
        }
        return conds;
    }

    /**
     * 将Relation类型为NOT IN的表达式 多个输入参数，进行展开。
     * 以AND（条件关系）进行拼接，NEQ类型的Relation
     * @param relation
     * @return
     */
    private static Condition convNotin2And(Relation relation) {
        assert relation.relation() == Condition.RelationType.NOT_IN;
        Object key = relation.key();
        @SuppressWarnings("unchecked")
        List<Object> values = (List<Object>) relation.value();
        Condition cond;
        Condition conds = null;
        for (Object value : values) {
            if (key instanceof HugeKeys) {
                cond = Condition.neq((HugeKeys) key, value);
            } else {
                cond = Condition.neq((Id) key, value);
            }
            conds = conds == null ? cond : Condition.and(conds, cond);
        }
        return conds;
    }

    /**
     * 将TEXT_CONTAINS_ANY 多个输入参数，展开。
     * 以OR 拼接，textContains 类型的Relation
     * @param relation
     * @return
     */
    private static Condition convTextContainsAny2Or(Relation relation) {
        assert relation.relation() == Condition.RelationType.TEXT_CONTAINS_ANY;
        @SuppressWarnings("unchecked")
        Collection<String> words = (Collection<String>) relation.value();
        Condition cond, conds = null;
        for (String word : words) {
            assert relation.key() instanceof Id;
            cond = Condition.textContains((Id) relation.key(), word);
            conds = conds == null ? cond : Condition.or(conds, cond);
        }
        return conds;
    }

    /**
     * 将所有类型condition展开，保证最下级都为RELATION，可直接用于计算。
     * 最后返回所有Relation堆叠的条件集合Set<Relations>
     * @param condition
     * @return
     */
    private static Set<Relations> flattenAndOr(Condition condition) {
        Set<Relations> result = InsertionOrderUtil.newSet();
        switch (condition.type()) {
            case RELATION:
                Relation relation = (Relation) condition;
                result.add(Relations.of(relation));
                break;
            case AND:
                Condition.And and = (Condition.And) condition;
                result = and(flattenAndOr(and.left()),
                             flattenAndOr(and.right()));
                break;
            case OR:
                Condition.Or or = (Condition.Or) condition;
                result = or(flattenAndOr(or.left()),
                            flattenAndOr(or.right()));
                break;
            default:
                throw new AssertionError(String.format(
                          "Wrong condition type: '%s'", condition.type()));
        }
        return result;
    }

    /**
     * 对left与right 做全排列，保证left与right逻辑条件同时满足
     * 计算量n*n
     * @param left
     * @param right
     * @return
     */
    private static Set<Relations> and(Set<Relations> left,
                                      Set<Relations> right) {
        Set<Relations> result = InsertionOrderUtil.newSet();
        for (Relations leftRelations : left) {
            for (Relations rightRelations : right) {
                Relations relations = new Relations();
                relations.addAll(leftRelations);
                relations.addAll(rightRelations);
                result.add(relations);
            }
        }
        return result;
    }

    /**
     * 将多left与right并集，满足一个即可
     * @param left
     * @param right
     * @return
     */
    private static Set<Relations> or(Set<Relations> left,
                                     Set<Relations> right) {
        Set<Relations> result = InsertionOrderUtil.newSet(left);
        result.addAll(right);
        return result;
    }

    /**
     * 由Relations创建ConditionQuery
     * @param query
     * @param relations
     * @return
     */
    private static ConditionQuery newQueryFromRelations(ConditionQuery query,
                                                        Relations relations) {
        ConditionQuery cq = query.copy();
        cq.resetConditions();
        for (Relation relation : relations) {
            cq.query(relation);
        }
        return cq;
    }

    /**
     * 优化计算条件，优化对象一个计算单元Relations
     * 将Relation.key 存在重复的场景，即(age>1 and age>2) -> (age>2)，进行优化。
     * key不同，无法优化。
     * 最后返回压缩后的Relations计算规则
     * @param relations
     * @return
     */
    private static Relations optimizeRelations(Relations relations) {
        // Optimize and-relations in one query
        // e.g. (age>1 and age>2) -> (age>2)
        Set<Object> keys = relations.stream().map(Relation::key)
                                    .collect(Collectors.toSet());

        // No duplicated keys
        if (keys.size() == relations.size()) {
            return relations;
        }

        for (Object key : keys) {
            // Get same key relations
            Relations rs = new Relations();
            for (Relation relation : relations) {
                if (relation.key().equals(key)) {
                    rs.add(relation);
                }
            }
            // Same key relation number is 1, needn't merge
            if (rs.size() == 1) {
                continue;
            }
            // Relations in rs having same key might need merge
            relations.removeAll(rs);
            rs = mergeRelations(rs);
            // Conditions are mutually exclusive(e.g. age>10 and age==9)
            if (rs.isEmpty()) {
                return null;
            }
            relations.addAll(rs);
        }

        return relations;
    }

    /**
     * Reduce and merge relations linked with 'AND' for same key
     * 目前对GT，GTE，EQ，LT，LTE做了优化
     * 优化逻辑：
     * 1.对数值计算进行优化，先将gt，gte，eq，lt，lte的计算类型保存起来，同时进行合并
     * 2.最后在将保存的gt，gte，eq，lt，lte，添加在最后的Relations
     * @param relations linked with 'AND' having same key, may contains 'in',
     *                 'not in', '>', '<', '>=', '<=', '==', '!='
     * @return merged relations
     */
    private static Relations mergeRelations(Relations relations) {
        Relations result = new Relations();
        boolean isNum = false;

        Relation gt = null;
        Relation gte = null;
        Relation eq = null;
        Relation lt = null;
        Relation lte = null;

        for (Relation relation : relations) {
            switch (relation.relation()) {
                case GT:
                    isNum = true;
                    //(age>1 and age>2) -> (age>2)
                    if (gt == null || compare(relation, gt) > 0) {
                        gt = relation;
                    }
                    break;
                case GTE:
                    isNum = true;
                    if (gte == null || compare(relation, gte) > 0) {
                        gte = relation;
                    }
                    break;
                case EQ:
                    if (eq == null) {
                        eq = relation;
                        if (eq.value() instanceof Number) {
                            isNum = true;
                        }
                        break;
                        //(age=1 and age=3) -> ()
                    } else if (!relation.value().equals(eq.value())) {
                        return Relations.of();
                    }
                    break;
                case LT:
                    isNum = true;
                    if (lt == null || compare(lt, relation) > 0) {
                        lt = relation;
                    }
                    break;
                case LTE:
                    isNum = true;
                    if (lte == null || compare(lte, relation) > 0) {
                        lte = relation;
                    }
                    break;
                default: // NEQ, IN, NOT_IN, CONTAINS, CONTAINS_KEY, SCAN
                    result.add(relation);
                    break;
            }
        }

        if (!isNum) {
            // Not number, only may have equal relation
            if (eq != null) {
                result.add(eq); //如果已经存在eq计算条件了，其他的条件已经没有意义了，result应该先清空
            }
        } else {
            //优化数值比较，并添加结果
            // At most have 1 eq, 1 lt, 1 lte, 1 gt, 1 gte
            result.addAll(calcValidRange(gte, gt, eq, lte, lt));
        }
        return result;
    }

    /**
     * 将范围区间计算进行合并，并检查有效性
     * Relations规则无效，返回EmptySet
     * 有效：存在eq，只返回eq
     * 则，返回lower与upper拼接的条件
     * @param gte
     * @param gt
     * @param eq
     * @param lte
     * @param lt
     * @return
     */
    private static Relations calcValidRange(Relation gte, Relation gt,
                                            Relation eq, Relation lte,
                                            Relation lt) {
        Relations result = new Relations();
        Relation lower = null;
        Relation upper = null;

        if (gte != null) {
            lower = gte;
        }
        if (gt != null) {
            // Select bigger one in (gt, gte) as lower limit
            lower = highRelation(gte, gt);
        }
        if (lt != null) {
            upper = lt;
        }
        if (lte != null) {
            // Select smaller one in (lt, lte) as upper limit
            upper = lowRelation(lte, lt);
        }
        // Ensure low < high
        if (!validRange(lower, upper)) {
            return Relations.of();
        }

        //如果存在eq，则判断条件是否有效，只返回eq
        // Handle eq
        if (eq != null) {
            if (!validEq(eq, lower, upper)) {
                return Relations.of();
            }
            //如果存在eq，则只返回eq
            result.add(eq);
            return result;
        }
        //添加lower与upper作为最后的条件
        // No eq if come here
        assert lower != null || upper != null;
        if (lower != null) {
            result.add(lower);
        }
        if (upper != null) {
            result.add(upper);
        }

        return result;
    }

    /**
     * 区间计算参数是否有效
     * @param low
     * @param high
     * @return
     */
    private static boolean validRange(Relation low, Relation high) {
        if (low == null || high == null) {
            return true;
        }
        return compare(low, high) < 0 || compare(low, high) == 0 &&
               low.relation() == Condition.RelationType.GTE &&
               high.relation() == Condition.RelationType.LTE;
    }

    /**
     * 判断EQ是否有效
     * @param eq
     * @param low
     * @param high
     * @return
     */
    private static boolean validEq(Relation eq, Relation low, Relation high) {
        // Ensure equalValue > lowValue
        if (low != null) {
            switch (low.relation()) {
                case GTE:
                    if (compare(eq, low) < 0) {
                        return false;
                    }
                    break;
                case GT:
                    if (compare(eq, low) <= 0) {
                        return false;
                    }
                    break;
                default:
                    throw new AssertionError("Must be GTE or GT");
            }
        }
        // Ensure equalValue < highValue
        if (high != null) {
            switch (high.relation()) {
                case LTE:
                    if (compare(eq, high) > 0) {
                        return false;
                    }
                    break;
                case LT:
                    if (compare(eq, high) >= 0) {
                        return false;
                    }
                    break;
                default:
                    throw new AssertionError("Must be LTE or LT");
            }
        }
        return true;
    }

    /**
     * 返回更大的Relation
     * @param first
     * @param second
     * @return
     */
    private static Relation highRelation(Relation first, Relation second) {
        return selectRelation(first, second, true);
    }

    private static Relation lowRelation(Relation first, Relation second) {
        return selectRelation(first, second, false);
    }

    /**
     * 比较first与second 两个Relation参数大小
     * @param first
     * @param second
     * @param high
     * @return
     */
    private static Relation selectRelation(Relation first, Relation second,
                                           boolean high) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        if (high) {
            if (compare(first, second) > 0) {
                return first;
            } else {
                return second;
            }
        } else {
            if (compare(first, second) < 0) {
                return first;
            } else {
                return second;
            }
        }
    }

    /**
     * 比较两个Relation参数大小，参数必须为数值类型
     * @param first
     * @param second
     * @return
     */
    private static int compare(Relation first, Relation second) {
        Object firstValue = first.value();
        Object secondValue = second.value();
        if (firstValue instanceof Number && secondValue instanceof Number) {
            return NumericUtil.compareNumber(firstValue, (Number) secondValue);
        } else if (firstValue instanceof Date && secondValue instanceof Date) {
            return ((Date) firstValue).compareTo((Date) secondValue);
        } else {
            throw new IllegalArgumentException(String.format(
                      "Can't compare between %s and %s", first, second));
        }
    }

    /**
     * Rename Relation Set to Relations to make code more readable
     * Use `LinkedHashSet` to keep relation in order with insertion
     * Relations 一个逻辑计算单元，内部所有逻辑必须同时true，and关系
     * 多个Relations之间关系为or，满足一个即可
     */
    private static class Relations extends LinkedHashSet<Relation> {

        private static final long serialVersionUID = -2110811280408887334L;

        public static Relations of(Relation... relations) {
            Relations rs = new Relations();
            rs.addAll(Arrays.asList(relations));
            return rs;
        }
    }
}
