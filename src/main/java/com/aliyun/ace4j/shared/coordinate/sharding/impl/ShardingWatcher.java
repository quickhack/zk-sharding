package com.aliyun.ace4j.shared.coordinate.sharding.impl;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.ace4j.shared.coordinate.sharding.ShardingInfo;
import com.aliyun.ace4j.shared.coordinate.sharding.ShardingListener;
import com.aliyun.ace4j.shared.coordinate.sharding.internal.ShardingRuleFactory;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorWatcher;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;

/**
 * @author ding.lid
 */
class ShardingWatcher {
    private static final Logger logger = LoggerFactory.getLogger(ShardingWatcher.class);
    private static final String GRAB_NODE_NAME = "grabs";

    public volatile CuratorFramework client;
    public volatile String shardingBaseDir;
    public volatile String key;

    public volatile ShardingInfo shardingInfo;

    private volatile String grabsNodeDir;

    private volatile boolean closed = false;

    public void init() throws Exception {
        grabsNodeDir = shardingBaseDir + "/" + key + "/" + GRAB_NODE_NAME;

        stateMachineExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    checking();
                } catch (Exception e) {
                    // FIXME checking 不应该出异常！
                    logger.error("error when checking of " + key + ", cause: " +
                            e.getMessage(), e);
                }
            }
        });
    }

    public void destroy() throws Exception {
        closed = true;
        try {
            stateMachineExecutor.shutdownNow();
        } catch (Exception e) {
            logger.error("error when close stateMachineExecutor, cause: " + e.getMessage(), e);
        }
        try {
            grabbingExecutor.shutdownNow();
        } catch (Exception e) {
            logger.error("error when close grabbingExecutor, cause: " + e.getMessage(), e);
        }
    }

    private CopyOnWriteArrayList<ShardingListener> listeners = new CopyOnWriteArrayList<ShardingListener>();

    public void addShardingListener(ShardingListener listener) {
        if (listeners.addIfAbsent(listener)) {  // 已经存在则忽略！
            safeNotify(listener); // FIXME Listener的同步，保证通知不漏！ 尽量不重！
        } else {
            throw new IllegalStateException("this ShardingListener is already added for sharding key " + key);
        }
    }

    public void removeShardingListener(ShardingListener listener) {
        listeners.remove(listener);
    }

    // =====================================
    // sharding statement machine:
    // checking -> grabbing -> grabbed
    // =====================================

    private final ExecutorService stateMachineExecutor = Executors.newFixedThreadPool(1); // 单线程切换状态！

    static enum Status {
        Checking, Grabbing, Grabbed
    }

    AtomicReference<Status> status = new AtomicReference<Status>(Status.Checking);

    // =====================================
    // 1. checking the shardingBaseDir node exists on zk， or zk is on!
    // =====================================

    void checking() {
        assert grabbedRuleNodeName == null;

        status.set(Status.Checking);
        while (!closed) {
            try {
                logger.info("checking " + grabsNodeDir);
                Stat stat = client.checkExists().forPath(grabsNodeDir);
                if (stat == null) {
                    logger.error("not exists grabs node " + grabsNodeDir + "on zookeeper!");
                    notifyListeners(null);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // ignore!
                    }
                } else {
                    logger.info("checking is ok (sharding key=" + key + ")!");
                    notifyListeners(new ShardingInfo(null, null)); // 连接ZooKeeper，并且有Sharding结点！

                    status.set(Status.Grabbing);
                    stateMachineExecutor.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                grabbing();
                            } catch (Exception e) {
                                // FIXME grabbing不应该出异常！
                                logger.error("error when grabbing of sharding key " + key + ", cause: " +
                                        e.getMessage(), e);
                            }
                        }
                    });
                    break;
                }
            } catch (Throwable t) {
                logger.error("Fail to check exists to sharding node " + grabsNodeDir +
                        ", cause: " + t.getMessage(), t);
            }
        }
    }

    // =====================================
    // 2. Grabbing
    // =====================================

    String grabbedRuleNodeName = null;

    void grabbing() throws Exception {
        logger.info("grabbing " + grabsNodeDir);

        assert grabbedRuleNodeName == null;

        List<String> ruleNodes = client.getChildren().usingWatcher(grabsNodeWatcher).forPath(grabsNodeDir);
        if (ruleNodes.isEmpty()) {
            logger.error("no rule nodes under grabs node(" + grabsNodeDir + ")");
        } else {
            Collections.shuffle(ruleNodes); // 乱序抢占，可以更多完成
        }

        grabbingFinished = new AtomicBoolean(false);
        ruleNode2GrabbingTask = new HashMap<String, GrabbingTask>();
        for (int i = 0; i < ruleNodes.size(); i++) {
            String ruleNode = ruleNodes.get(i);

            GrabbingTask task = new GrabbingTask(5 * i, ruleNode, grabbingFinished);
            grabbingExecutor.execute(task);
            ruleNode2GrabbingTask.put(ruleNode, task);
        }
    }

    CuratorWatcher grabsNodeWatcher = new CuratorWatcher() {
        @Override
        public void process(WatchedEvent event) throws Exception {
            logger.info("Receive grabs node event: " + event);

            switch (event.getType()) {
                case None:
                    break;
                case NodeDataChanged:
                    // ignore
                    break;
                case NodeDeleted:
                    logger.error("Grabs node deleted, resetToChecking!");
                    resetToChecking(false);
                    return;
                case NodeChildrenChanged:
                    if (status.compareAndSet(Status.Grabbing, Status.Checking)) {
                        logger.warn("NodeChildrenChanged of grabs node when Grabbing, resetToChecking!");
                        resetToChecking(false);
                        return;
                    }
            }

            // 需要自己再次订阅！
            if (status.get() == Status.Grabbing) {
                client.getChildren().usingWatcher(this).forPath(grabsNodeDir);
            }
        }
    };
    Map<String, GrabbingTask> ruleNode2GrabbingTask;
    volatile AtomicBoolean grabbingFinished;

    private final ExecutorService grabbingExecutor = Executors.newCachedThreadPool();

    /**
     * 抢Rule Node并等待，直到 成功 或 没有必要了（其它的GrabbingTask已经抢到了）！
     */
    class GrabbingTask implements Runnable {
        final int sleep;
        final String ruleNode;
        final AtomicBoolean finished;
        final CountDownLatch latch = new CountDownLatch(1);

        GrabbingTask(int sleep, String ruleNode, AtomicBoolean finished) {
            this.sleep = sleep;
            this.ruleNode = ruleNode;
            this.finished = finished;
        }

        @Override
        public void run() {
            if (sleep > 0) {
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    // ignore
                }
            }

            logger.info("grabbing in task, " + grabsNodeDir + "/" + ruleNode);
            while (!finished.get() && !closed) {
                InterProcessMutex lock = new InterProcessMutex(client, grabsNodeDir + "/" + ruleNode);
                boolean acquire = false;
                try {
                    acquire = lock.acquire(1, TimeUnit.SECONDS); // TODO 确定一个合适的等待时长！
                } catch (Exception e) {
                    // FIXME 如何处理异常？！ ignore!
                }

                if (acquire) {
                    try {
                        if (finished.compareAndSet(false, true) &&
                                status.compareAndSet(Status.Grabbing, Status.Grabbed)) {
                            grabbedRuleNodeName = ruleNode;
                            logger.info("grabbing is done!");

                            stateMachineExecutor.execute(new Runnable() {
                                @Override
                                public void run() {
                                    grabbed();
                                }
                            });

                            try {
                                latch.await();
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }
                    } finally { // 一定要lock.release！
                        try {
                            lock.release();
                        } catch (Exception e) {
                            // FIXME 如何处理？ 重试逻辑会不会导致Lock会再次获得？！
                            logger.warn("Exception when release lock(sharding key: " + key +
                                    "): " + e.getMessage(), e);
                        }
                    }
                }
            }
        }
    }

    // =====================================
    // 3 Grabbed
    // =====================================

    /*
     * 这个方法是没有Block的
     */
    void grabbed() {
        logger.info("grabbed! grabbedNodeName: " + grabsNodeDir + "/" + grabbedRuleNodeName);
        try {
            String path = grabsNodeDir + "/" + grabbedRuleNodeName;
            byte[] data = client.getData().usingWatcher(grabbedRuleNodeWatcher).forPath(path);
            notifyListeners(data2ShardingInfo(path, data));
        } catch (Exception e) {
            logger.error("Fail to get data of rule node when do grabbed, resetToChecking, cause: " +
                    e.getMessage(), e);
            resetToChecking(false);
        }
    }

    CuratorWatcher grabbedRuleNodeWatcher = new CuratorWatcher() {
        @Override
        public void process(WatchedEvent event) throws Exception {
            logger.info("grabbedRuleNode receive event: " + event);

            final String path = event.getPath();
            switch (event.getType()) {
                case None:
                    if (event.getState() == Watcher.Event.KeeperState.Disconnected) {
                        logger.error("disconnected from zk after grabbed, resetToChecking!");
                        resetToChecking(true);
                        return;
                    }
                    break;
                case NodeDataChanged:
                    byte[] data = client.getData().forPath(path);
                    notifyListeners(data2ShardingInfo(path, data));
                    break;
                case NodeDeleted: // 被删除了？退出Grabbed状态！
                    logger.error("grabbed rule node is deleted after grabbed, resetToChecking!");
                    resetToChecking(false);
                    return;
            }

            client.getData().usingWatcher(this).forPath(path);
        }
    };

    // =====================================
    // State Machine Helpers
    // =====================================

    static ShardingInfo data2ShardingInfo(String path, byte[] data) {
        if (data != null) {
            if (data.length > 0) {
                try {
                    String rule = new String(data, Charset.forName("utf-8"));
                    rule = rule.trim();
                    if (rule.trim().length() >= 0) {
                        return new ShardingInfo(ShardingRuleFactory.toShardingRule(rule), rule);
                    } else {
                        logger.error("blank data in rule node " + path);
                    }
                } catch (Exception e) {
                    logger.error("Fail to create sharding rule from rule node " + path +
                            ", cause: " + e.getMessage(), e);
                }
            } else {
                logger.error("NO data in rule node " + path);
            }
        }

        return new ShardingInfo(null, null);
    }

    static boolean shardingInfoEquals(ShardingInfo oldInfo, ShardingInfo newInfo) {
        if (oldInfo == null && newInfo == null) return true;
        if (oldInfo == null || newInfo == null) return false;

        return newInfo.isDisabled() == oldInfo.isDisabled() &&
                StringUtils.equals(newInfo.getRule(), oldInfo.getRule());
    }

    private void safeNotify(ShardingListener listener) {
        try {
            listener.shardingChanged(shardingInfo);
        } catch (Exception e) {
            logger.warn("Error when notify sharding listener of key " + key +
                    ", ignored. Cause: " + e.getMessage(), e);
        }
    }

    void notifyListeners(ShardingInfo info) {
        if (shardingInfoEquals(shardingInfo, info)) return;

        shardingInfo = info;
        for (ShardingListener listener : listeners) {
            safeNotify(listener);
        }
    }

    synchronized void resetToChecking(boolean isZooKeeperError) {
        status.set(Status.Checking);

        String name = grabbedRuleNodeName;
        grabbedRuleNodeName = null;

        // release in GrabbingTask! 
        if (ruleNode2GrabbingTask != null) {
            GrabbingTask grabbingTask = ruleNode2GrabbingTask.get(name);
            if (grabbingTask != null)
                grabbingTask.latch.countDown();
            ruleNode2GrabbingTask = null;
        }

        // FIXME 要有事件通知！ 下面的通知是否合适？
        if (isZooKeeperError) {
            notifyListeners(null);
        } else {
            notifyListeners(new ShardingInfo(null, null));
        }

        stateMachineExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    checking();
                } catch (Exception e) {
                    // FIXME 异常的处理策略是？
                    logger.warn("Error when checking sharding of key " + key +
                            ", ignored. Cause: " + e.getMessage(), e);
                }
            }
        });
    }
}
