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

package com.baidu.hugegraph.backend.cache;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.concurrent.KeyLock;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class RamCache implements Cache {

    public static final int MB = 1024 * 1024;
    public static final int DEFAULT_SIZE = 1 * MB;
    public static final int MAX_INIT_CAP = 100 * MB;

    private static final Logger LOG = Log.logger(Cache.class);

    private volatile long hits = 0L;
    private volatile long miss = 0L;

    // Default expire time(ms)
    private volatile long expire = 0L;

    // NOTE: the count in number of items, not in bytes
    private final int capacity;
    private final int halfCapacity;

    // Implement LRU cache
    private final ConcurrentMap<Id, LinkNode<Id, Object>> map;
    private final LinkedQueueNonBigLock<Id, Object> queue;

    private final KeyLock keyLock;

    public RamCache() {
        this(DEFAULT_SIZE);
    }

    public RamCache(int capacity) {
        if (capacity < 0) {
            capacity = 0;
        }
        this.keyLock = new KeyLock();
        this.capacity = capacity;
        this.halfCapacity = this.capacity >> 1;
        // capacity 为预估缓存元素个数
        // initialCapacity 为预估bulk个数，为了保证ConcurrentHashMap性能
        //capacity 如果大于等于 1MB个，则除以1024，每个bulk存放1024个对象
        //如果小于1MB个，则只有256个bulk，则每个bulk存放4096
        //bulk个数 最大100MB个
        int initialCapacity = capacity >= MB ? capacity >> 10 : 256;
        if (initialCapacity > MAX_INIT_CAP) {
            initialCapacity = MAX_INIT_CAP;
        }

        this.map = new ConcurrentHashMap<>(initialCapacity);
        this.queue = new LinkedQueueNonBigLock<>();
    }

    /**
     * 查询id对应的缓存
     * 如果map容量超过half，则将访问过的对象移到queue末尾
     * @param id
     * @return
     */
    @Watched(prefix = "ramcache")
    private final Object access(Id id) {
        assert id != null;
        //缓存空间一般时
        // 直接查询map
        // 不修改LRU链  // NOTE: update the queue only if the size > capacity/2
        if (this.map.size() <= this.halfCapacity) {
            LinkNode<Id, Object> node = this.map.get(id);
            if (node == null) {
                return null;
            }
            assert id.equals(node.key());
            return node.value();
        }
        //当容量大于一半时，会有数据删除和修改，需要锁保证
        final Lock lock = this.keyLock.lock(id);
        try {
            LinkNode<Id, Object> node = this.map.get(id);
            if (node == null) {
                return null;
            }

            // NOTE: update the queue only if the size > capacity/2
            if (this.map.size() > this.halfCapacity) {
                // Move the node from mid to tail
                if (this.queue.remove(node) == null) {
                    // The node may be removed by others through dequeue()
                    // 什么情况?? --》map中的数据与queue中不一致。存在map中存在，但queue中不存在的情况
                    return null; //如果被删除了，则直接返回null，不会放回到queue中
                }
                //如果queue中存在，则移到尾部
                this.queue.enqueue(node);
            }

            assert id.equals(node.key());
            return node.value();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 将对象写入缓存末尾，或更新已有的id并写入末尾
     * @param id
     * @param value
     */
    @Watched(prefix = "ramcache")
    private final void write(Id id, Object value) {
        assert id != null;
        assert this.capacity > 0;

        final Lock lock = this.keyLock.lock(id);
        try {
            // The cache is full
            while (this.map.size() >= this.capacity) {
                /*
                 * Remove the oldest from the queue
                 * NOTE: it maybe return null if someone else (that's other
                 * threads) are doing dequeue() and the queue may be empty.
                 */
                //因为dequeue使用while方式获取lock，会存在让出线程时间，被其他线程使用，并且只有
                //queue空的时候才会返回null
                LinkNode<Id, Object> removed = this.queue.dequeue();
                if (removed == null) {
                    /*
                     * If at this time someone add some new items, these will
                     * be cleared in the map, but still stay in the queue, so
                     * the queue will have some more nodes than the map.
                     */
                    this.map.clear(); //queue 数据多于map
                    break;
                }
                /*
                 * Remove the oldest from the map
                 * NOTE: it maybe return null if other threads are doing remove
                 */
                this.map.remove(removed.key());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("RamCache replaced '{}' with '{}' (capacity={})",
                              removed.key(), id, this.capacity);
                }
                /*
                 * Release the object
                 * NOTE: we can't reuse the removed node due to someone else
                 * may access the node (will do remove() -> enqueue())
                 */
                removed = null;
            }
            //删除原有记录
            // Remove the old node if exists
            LinkNode<Id, Object> node = this.map.get(id);
            if (node != null) {
                this.queue.remove(node);
            }
            //写入队列尾部
            // Add the new item to tail of the queue, then map it
            this.map.put(id, this.queue.enqueue(id, value));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 删除缓存
     * @param id
     */
    @Watched(prefix = "ramcache")
    private final void remove(Id id) {
        if (id == null) {
            return;
        }
        assert id != null;

        final Lock lock = this.keyLock.lock(id);
        try {
            /*
             * Remove the id from map and queue
             * NOTE: it maybe return null if other threads have removed the id
             */
            LinkNode<Id, Object> node = this.map.remove(id);
            if (node != null) {
                this.queue.remove(node);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 查询缓存，统计hits与miss
     * @param id
     * @return
     */
    @Watched(prefix = "ramcache")
    @Override
    public Object get(Id id) {
        if (id == null) {
            return null;
        }
        Object value = null;
        /*
        map容量小于一半，可以直接查询，开销不大
        map容量大于一半,查询开销大，先初略判断map中是否有缓存
        ，如果map中没有则一定没有，再access查询缓存value，
        可能返回null，因为在并发环境中，存在查询时被其他线程
        删除的可能
         */
        //containsKey避免没在缓存中，还要access（lock开销大）
        //为什么不直接map.get 因为需要access维护LRU链的功能
        if (this.map.size() <= this.halfCapacity || this.map.containsKey(id)) {
            // Maybe the id removed by other threads and returned null value
            value = this.access(id);
        }

        if (value == null) {
            ++this.miss;
            if (LOG.isDebugEnabled()) {
                LOG.debug("RamCache missed '{}' (miss={}, hits={})",
                          id, this.miss, this.hits);
            }
        } else {
            ++this.hits;
            if (LOG.isDebugEnabled()) {
                LOG.debug("RamCache cached '{}' (hits={}, miss={})",
                          id, this.hits, this.miss);
            }
        }
        return value;
    }

    /**
     * 查询缓存，不存在则调用fetcher获取对象
     * @param id
     * @param fetcher
     * @return
     */
    @Watched(prefix = "ramcache")
    @Override
    public Object getOrFetch(Id id, Function<Id, Object> fetcher) {
        if (id == null) {
            return null;
        }
        Object value = null;
        if (this.map.size() <= this.halfCapacity || this.map.containsKey(id)) {
            // Maybe the id removed by other threads and returned null value
            value = this.access(id);
        }

        if (value == null) {
            ++this.miss;
            if (LOG.isDebugEnabled()) {
                LOG.debug("RamCache missed '{}' (miss={}, hits={})",
                          id, this.miss, this.hits);
            }
            // Do fetch and update the cache
            value = fetcher.apply(id);
            this.update(id, value);
        } else {
            ++this.hits;
            if (LOG.isDebugEnabled()) {
                LOG.debug("RamCache cached '{}' (hits={}, miss={})",
                          id, this.hits, this.miss);
            }
        }
        return value;
    }

    @Watched(prefix = "ramcache")
    @Override
    public void update(Id id, Object value) {
        if (id == null || value == null || this.capacity <= 0) {
            return;
        }
        this.write(id, value);
    }

    @Watched(prefix = "ramcache")
    @Override
    public void updateIfAbsent(Id id, Object value) {
        if (id == null || value == null ||
            this.capacity <= 0 || this.map.containsKey(id)) {
            return;
        }
        this.write(id, value);
    }

    @Watched(prefix = "ramcache")
    @Override
    public void updateIfPresent(Id id, Object value) {
        if (id == null || value == null ||
            this.capacity <= 0 || !this.map.containsKey(id)) {
            return;
        }
        this.write(id, value);
    }

    @Watched(prefix = "ramcache")
    @Override
    public void invalidate(Id id) {
        if (id == null || !this.map.containsKey(id)) {
            return;
        }
        this.remove(id);
    }

    @Watched(prefix = "ramcache")
    @Override
    public void traverse(Consumer<Object> consumer) {
        E.checkNotNull(consumer, "consumer");
        // NOTE: forEach is 20% faster than for-in with ConcurrentHashMap
        this.map.values().forEach(node -> consumer.accept(node.value()));
    }

    @Watched(prefix = "ramcache")
    @Override
    public void clear() {
        // TODO: synchronized
        if (this.capacity <= 0 || this.map.isEmpty()) {
            return;
        }
        this.map.clear();
        this.queue.clear();
    }

    @Override
    public void expire(long seconds) {
        // Convert the unit from seconds to milliseconds
        this.expire = seconds * 1000;
    }

    @Override
    public long expire() {
        return this.expire;
    }

    /**
     * 清理过期缓存，需要手动触发
     * @return 清理缓存个数
     */
    @Override
    public long tick() {
        long expireTime = this.expire;
        if (expireTime <= 0) {
            return 0L;
        }

        int expireItems = 0;
        long current = now();
        for (LinkNode<Id, Object> node : this.map.values()) {
            if (current - node.time() > expireTime) {
                // Remove item while iterating map (it must be ConcurrentMap)
                this.remove(node.key());
                expireItems++;
            }
        }

        if (expireItems > 0) {
            LOG.debug("Cache expired {} items cost {}ms (size {}, expire {}ms)",
                      expireItems, now() - current, this.size(), expireTime);
        }
        return expireItems;
    }

    /**
     * 缓存容量 元素个数
     * @return
     */
    @Override
    public long capacity() {
        return this.capacity;
    }

    /**
     * 缓存中实际 元素个数
     * @return
     */
    @Override
    public long size() {
        return this.map.size();
    }

    @Override
    public long hits() {
        return this.hits;
    }

    @Override
    public long miss() {
        return this.miss;
    }

    @Override
    public String toString() {
        return this.map.toString();
    }

    private static final long now() {
        return System.currentTimeMillis();
    }

    private static class LinkNode<K, V> {

        private final K key;
        private final V value;
        private long time;
        private LinkNode<K, V> prev;
        private LinkNode<K, V> next;

        public LinkNode(K key, V value) {
            assert key != null;
            this.time = now();
            this.key = key;
            this.value = value;
            this.prev = this.next = null;
        }

        public final K key() {
            return this.key;
        }

        public final V value() {
            return this.value;
        }

        public long time() {
            return this.time;
        }

        @Override
        public String toString() {
            return this.key.toString();
        }

        @Override
        public int hashCode() {
            return this.key.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof LinkNode)) {
                return false;
            }
            @SuppressWarnings("unchecked")
            LinkNode<K, V> other = (LinkNode<K, V>) obj;
            return this.key.equals(other.key());
        }
    }

    private static final class LinkedQueueNonBigLock<K, V> {

        private final KeyLock keyLock;
        private final LinkNode<K, V> empty;
        private final LinkNode<K, V> head;
        private final LinkNode<K, V> rear;
        // private volatile long size;

        @SuppressWarnings("unchecked")
        public LinkedQueueNonBigLock() {
            this.keyLock = new KeyLock();
            this.empty = new LinkNode<>((K) "<empty>", null);
            this.head = new LinkNode<>((K) "<head>", null);
            this.rear = new LinkNode<>((K) "<rear>", null);

            this.reset();
        }

        /**
         * Reset the head node and rear node
         * NOTE:
         *  only called by LinkedQueueNonBigLock() without lock
         *  or called by clear() with lock(head, rear)
         */
        private void reset() {
            this.head.prev = this.empty;
            this.head.next = this.rear;

            this.rear.prev = this.head;
            this.rear.next = this.empty;

            assert this.head.next == this.rear;
            assert this.rear.prev == this.head;
        }

        /**
         * Dump keys of all nodes in this queue (just for debug)
         * 由head开始向后遍历所有node，返回keys
         */
        private List<K> dumpKeys() {
            List<K> keys = new LinkedList<>();
            LinkNode<K, V> node = this.head.next;
            while (node != this.rear && node != this.empty) {
                assert node != null;
                keys.add(node.key());
                node = node.next;
            }
            return keys;
        }

        /**
         * Check whether a key not in this queue (just for debug)
         */
        @SuppressWarnings("unused")
        private boolean checkNotInQueue(K key) {
            List<K> keys = this.dumpKeys();
            if (keys.contains(key)) {
                throw new RuntimeException(String.format(
                          "Expect %s should be not in %s", key, keys));
            }
            return true;
        }

        /**
         * Check whether there is circular reference (just for debug)
         * NOTE: but it is important to note that this is only key check
         * rather than pointer check.
         * 查找循环引用
         */
        @SuppressWarnings("unused")
        private boolean checkPrevNotInNext(LinkNode<K, V> self) {
            LinkNode<K, V> prev = self.prev;
            if (prev.key() == null) {
                assert prev == this.head || prev == this.empty : prev;
                return true;
            }
            List<K> keys = this.dumpKeys();
            int prevPos = keys.indexOf(prev.key());
            int selfPos = keys.indexOf(self.key());
            //单项有序链 判断循环引用方法，prevPos必须在selfPos前面
            if (prevPos > selfPos && selfPos != -1) {
                throw new RuntimeException(String.format(
                          "Expect %s should be before %s, actual %s",
                          prev.key(), self.key(), keys));
            }
            return true;
        }

        private List<Lock> lock(Object... nodes) {
            return this.keyLock.lockAll(nodes);
        }

        private List<Lock> lock(Object node1, Object node2) {
            return this.keyLock.lockAll(node1, node2);
        }

        private void unlock(List<Lock> locks) {
            this.keyLock.unlockAll(locks);
        }

        /**
         * Clear the queue
         */
        public void clear() {
            assert this.rear.prev != null : this.head.next;

            while (true) {
                /*
                 * If someone is removing the last node by remove(),
                 * it will update the rear.prev, so we should lock it.
                 * 如果有人正在remove最后的元素，则rear.prev将会被修改，所以需要lock last元素
                 */
                LinkNode<K, V> last = this.rear.prev;
                //锁定 防止clear过程中，竞争访问this.head, last, this.rear
                List<Lock> locks = this.lock(this.head, last, this.rear);
                try {
                    if (last != this.rear.prev) {
                        //同步之前可能有变动，步骤，释放锁（有可能其他线程在做相同的事情），更新last，重新lock
                        // The rear.prev has changed, try to get lock again
                        continue;
                    }
                    //清空queue
                    this.reset();
                } finally {
                    this.unlock(locks);
                }
                return;
            }
        }

        /**
         * Add an item with key-value to the queue
         */
        public LinkNode<K, V> enqueue(K key, V value) {
            return this.enqueue(new LinkNode<>(key, value));
        }

        /**
         * Add a node to tail of the queue
         */
        public LinkNode<K, V> enqueue(LinkNode<K, V> node) {
            assert node != null;
            assert node.prev == null || node.prev == this.empty;
            assert node.next == null || node.next == this.empty;

            while (true) {
                LinkNode<K, V> last = this.rear.prev;

                // TODO: should we lock the new `node`?
                List<Lock> locks = this.lock(last, this.rear);
                try {
                    if (last != this.rear.prev) {
                        // The rear.prev has changed, try to get lock again
                        continue;
                    }

                    /*
                     * Link the node to the rear before to the last if we
                     * have not locked the node itself, because dumpKeys()
                     * may get the new node with next=null.
                     * TODO: it also depends on memory barrier.
                     */

                    // Build the link between `node` and the rear
                    node.next = this.rear;
                    assert this.rear.prev == last : this.rear.prev;
                    this.rear.prev = node;

                    // Build the link between `last` and `node`
                    node.prev = last;
                    last.next = node;

                    return node;
                } finally {
                    this.unlock(locks);
                }
            }
        }

        /**
         * Remove a node from head of the queue
         */
        public LinkNode<K, V> dequeue() {
            while (true) {
                LinkNode<K, V> first = this.head.next;
                if (first == this.rear) {
                    // Empty queue
                    return null;
                }

                //不需要lock  first.next吗？
                //因为queue的操作是有规则的，remove某个node时，必须获取prev的lock，
                // 这样当有线程要修改first.next时，必须先lock first，每一个操作都会
                //判断前一个元素状态，就能起到lock隔离的作用
                //同时node.prev的修改只能由 前一个元素进行，不会有其他情况，所以有了前一个元素的锁，
                // 就能隔离对node.prev的操作
                List<Lock> locks = this.lock(this.head, first);
                try {
                    if (first != this.head.next) {
                        // The head.next has changed, try to get lock again
                        continue;
                    }

                    // Break the link between the head and `first`
                    assert first.next != null;
                    this.head.next = first.next;

                    first.next.prev = this.head;

                    // Clear the links of the first node
                    first.prev = this.empty;
                    first.next = this.empty;

                    return first;
                } finally {
                    this.unlock(locks);
                }
            }
        }

        /**
         * Remove a specified node from the queue
         */
        public LinkNode<K, V> remove(LinkNode<K, V> node) {
            assert node != this.empty;
            assert node != this.head && node != this.rear;

            while (true) {
                //没有同步，不确定node.prev返回的正确的
                LinkNode<K, V> prev = node.prev;
                if (prev == this.empty) {
                    assert node.next == this.empty;
                    // Ignore the node if it has been removed
                    return null;
                }
                //同步后，比较数据是否一致，如果不一致重新获取
                List<Lock> locks = this.lock(prev, node);
                try {
                    if (prev != node.prev) {
                        /*
                         * The previous node has changed (maybe it's lock
                         * released after it's removed, then we got the
                         * lock), so try again until it's not changed.
                         */
                        continue;
                    }
                    assert node.next != null : node;
                    assert node.next != node.prev : node.next;

                    // Build the link between node.prev and node.next
                    node.prev.next = node.next;
                    node.next.prev = node.prev;

                    assert prev == node.prev : prev.key + "!=" + node.prev;

                    // Clear the links of `node`
                    node.prev = this.empty;
                    node.next = this.empty;

                    return node;
                } finally {
                    this.unlock(locks);
                }
            }
        }
    }
}
