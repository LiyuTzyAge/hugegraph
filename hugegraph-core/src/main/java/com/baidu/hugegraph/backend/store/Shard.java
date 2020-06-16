/*
 * Copyright (C) 2018 Baidu, Inc. All Rights Reserved.
 */

package com.baidu.hugegraph.backend.store;

/**
 * Shard is used for backend storage (like cassandra, hbase) scanning
 * operations. Each shard represents a range of tokens for a node.
 * Reading data from a given shard does not cross multiple nodes.
 *
 * 为了范围扫描提供优化，将相邻数据放在同一个node，防止扫描过多nodes
 * shard代表一个node的范围
 * 使用场景：元数据id扫描，Condition.scan
 */
public class Shard {

    // token range start =int
    private String start;
    // token range end =int
    private String end;
    // partitions count in this range
    private long length;

    public Shard(String start, String end, long length) {
        this.start = start;
        this.end = end;
        this.length = length;
    }

    public String start() {
        return this.start;
    }

    public void start(String start) {
        this.start = start;
    }

    public String end() {
        return this.end;
    }

    public void end(String end) {
        this.end = end;
    }

    public long length() {
        return this.length;
    }

    public void length(long length) {
        this.length = length;
    }

    @Override
    public String toString() {
        return String.format("Shard{start=%s, end=%s, length=%s}",
                             this.start, this.end, this.length);
    }
}