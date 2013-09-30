package com.aliyun.ace4j.shared.coordinate.sharding.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.ace4j.shared.coordinate.sharding.ShardingInfo;
import com.aliyun.ace4j.shared.coordinate.sharding.ShardingListener;
import com.aliyun.ace4j.shared.coordinate.sharding.ShardingService;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.netflix.curator.retry.ExponentialBackoffRetry;

/**
 * @author ding.lid
 */
public class ShardingServiceImpl implements ShardingService {
    private static final Logger logger = LoggerFactory.getLogger(ShardingServiceImpl.class);

    private String keys;
    List<String> keyList = new ArrayList<String>();
    private String zkAddress;
    private String shardingBaseDir;

    public String getKeys() {
        return keys;
    }

    private static final Pattern KEY_SEP = Pattern.compile("\\s*,\\s*");

    static List<String> splitToKeyList(String keys) {
        keys = keys.trim();
        List<String> list = new ArrayList<String>();
        String[] split = KEY_SEP.split(keys);
        for (String s : split) {
            if (s.length() > 0) {
                if (s.contains("/")) {
                    throw new IllegalStateException("key " + s + " contains '/' in " + keys);
                }
                list.add(s);
            }
        }
        return list;
    }

    public void setKeys(String keys) {
        if (StringUtils.isEmpty(keys) || "N/A".equalsIgnoreCase(keys.trim())) {
            logger.warn("No Sharding keys is set!!!");
            return;
        }
        this.keys = keys.trim();

        keyList = splitToKeyList(keys);
        if (keyList.isEmpty()) {
            logger.warn("No Sharding keys is set!!!");
        }
    }

    public void setShardingBaseDir(String shardingBaseDir) {
        this.shardingBaseDir = shardingBaseDir;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    ConnectionState connectionState;

    ConnectionStateListener connectionStateListener = new ConnectionStateListener() {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
//            // TODO 目前ShardingWatcher自己的Watcher可以完成resetToChecking操作！
//            if (newState == ConnectionState.SUSPENDED || newState == ConnectionState.LOST) {
//                for (Map.Entry<String, ShardingWatcher> entry : key2Watcher.entrySet()) {
//                    logger.error("Zk state " + newState + ", resetToChecking " + entry.getKey());
//                    entry.getValue().resetToChecking();
//                }
//            }
            connectionState = newState;
            logger.info("State change: " + newState);
        }
    };

    private CuratorFramework client;
    private Map<String, ShardingWatcher> key2Watcher = new HashMap<String, ShardingWatcher>();

    public synchronized void init() throws Exception {
        if (keyList == null || keyList.isEmpty()) {
            //throw new IllegalStateException("key is empty!");
            return;
        }
        if (StringUtils.isEmpty(zkAddress)) {
            throw new IllegalStateException("zkAddress is empty!");
        }
        if (StringUtils.isEmpty(shardingBaseDir)) {
            throw new IllegalStateException("shardingBaseDir is empty!");
        }

        // 一直重试；最长sleep时间是2分钟。
        // FIXME 重试是指什么的重试？Zk所有操作的重试？只是Zk连接的重试？ 这会影响如何配置
        client = CuratorFrameworkFactory.newClient(zkAddress, new ExponentialBackoffRetry(1000, Integer.MAX_VALUE, 2 * 60 * 1000));
        client.getConnectionStateListenable().addListener(connectionStateListener);
        client.start();

        for (String key : keyList) {
            ShardingWatcher watcher = new ShardingWatcher();
            watcher.client = client;
            watcher.shardingBaseDir = shardingBaseDir;
            watcher.key = key;
            watcher.init();

            key2Watcher.put(key, watcher);
        }

        // 等一下。这样在正常情况下，使用ShardingService都不用考虑还没有连接上ZooKeeper的Case。
        allWatcherGetResultOrTimeout();

        // FIXME addShutdownHook，SofaX Fix了Destroy-Method后，不需要addShutdownHook代码！
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                ShardingServiceImpl.this.destroy();
            }
        });
    }

    void allWatcherGetResultOrTimeout() {
        logger.info("Wait some seconds for watcher to node from zookeeper...");

        final long tick = System.currentTimeMillis();
        LBL_WHILE:
        while (System.currentTimeMillis() - tick < 3 * 1000) { // 等个3秒钟！
            for (Map.Entry<String, ShardingWatcher> entry : key2Watcher.entrySet()) {
                if (entry.getValue().shardingInfo == null) {
                    try {
                        Thread.sleep(10);
                        continue LBL_WHILE;
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
            return;
        }
    }

    private volatile boolean destroyed = false;

    public synchronized void destroy() {
        if (destroyed) return;
        destroyed = true;

        try {
            for (Map.Entry<String, ShardingWatcher> entry : key2Watcher.entrySet()) {
                entry.getValue().destroy();
            }
            if (client != null) client.close();
        } catch (Exception e) {
            logger.warn("Error when close zookeeper client: " + e.getMessage(), e);
        }
    }

    String getDefaultKey() {
        if (keyList.isEmpty()) {
            throw new IllegalStateException("NO Sharding key is configured!");
        }
        return keyList.get(0);
    }

    ShardingWatcher getWatcherOfKey(String key) {
        ShardingWatcher watcher = key2Watcher.get(key);
        if (watcher == null) {
            throw new IllegalStateException("Sharding key(" + key + ") is NOT configured!");
        }
        return watcher;
    }

    @Override
    public ShardingInfo getShardingInfo() {
        return getShardingInfo(getDefaultKey());
    }
    
    public ShardingInfo getShardingInfo(String key) {
        return getWatcherOfKey(key).shardingInfo;
    }

    @Override
    public void addShardingListener(ShardingListener listener) {
        addShardingListener(getDefaultKey(), listener);
    }

    public void addShardingListener(String key, ShardingListener listener) {
        getWatcherOfKey(key).addShardingListener(listener);
    }

    @Override
    public void removeShardingListener(ShardingListener listener) {
        removeShardingListener(getDefaultKey(), listener);
    }

    public void removeShardingListener(String key, ShardingListener listener) {
        getWatcherOfKey(key).removeShardingListener(listener);
    }
}
