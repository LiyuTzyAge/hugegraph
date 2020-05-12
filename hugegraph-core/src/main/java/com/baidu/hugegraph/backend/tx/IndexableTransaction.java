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

package com.baidu.hugegraph.backend.tx;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;

public abstract class IndexableTransaction extends AbstractTransaction {

    public IndexableTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);
    }

    /**
     * 判断index transaction中是否有为提交项
     * @return
     */
    @Override
    public boolean hasUpdates() {
        AbstractTransaction indexTx = this.indexTransaction();
        boolean indexTxChanged = (indexTx != null && indexTx.hasUpdates());
        return indexTxChanged || super.hasUpdates();
    }

    @Override
    protected void reset() {
        super.reset();

        // It's null when called by super AbstractTransaction()
        AbstractTransaction indexTx = this.indexTransaction();
        if (indexTx != null) {
            indexTx.reset();
        }
    }

    /**
     * 提交graph/schema + index 事务
     */
    @Override
    protected void commit2Backend() {
        //graph/schema updates
        BackendMutation mutation = this.prepareCommit();
        //index updates
        BackendMutation txMutation = this.indexTransaction().prepareCommit();
        assert !mutation.isEmpty() || !txMutation.isEmpty();
        // Commit graph/schema updates and index updates with graph/schema tx
        this.commitMutation2Backend(mutation, txMutation);
    }

    @Override
    public void commitIfGtSize(int size) throws BackendException {
        int totalSize = this.mutationSize() +
                        this.indexTransaction().mutationSize();
        if (totalSize >= size) {
            this.commit();
        }
    }

    /**
     * rollback graph/schema + index 事务
     * @throws BackendException
     */
    @Override
    public void rollback() throws BackendException {
        try {
            super.rollback();
        } finally {
            this.indexTransaction().rollback();
        }
    }

    @Override
    public void close() {
        try {
            this.indexTransaction().close();
        } finally {
            super.close();
        }
    }

    /**
     * 创建index transaction
     * index transaction 内嵌在graph/schema 事务中
     * @return
     */
    protected abstract AbstractTransaction indexTransaction();
}
