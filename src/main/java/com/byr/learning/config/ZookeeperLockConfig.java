package com.byr.learning.config;

import org.apache.zookeeper.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @Author: yanrong
 */
@Component
public class ZookeeperLockConfig {
    @Autowired
    ConfigProperties configProperties;
    /**
     * 根节点
     */
    final static String PARENT_NODE = "/byr";

    @Bean
    public ZooKeeper init() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
//             创建连接，zkAddress格式为：IP:PORT
            ZooKeeper zooKeeper = new ZooKeeper(configProperties.getConnectString(),
                    configProperties.getConnectionTimeoutMs(), new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        //减少锁存器的计数，如果存在则释放所有等待线程,计数为零
                        countDownLatch.countDown();
                    }
                }
            });
            //等到当前线程结束
            countDownLatch.await();
            if (zooKeeper.exists(PARENT_NODE, false) == null) {
                zooKeeper.create(PARENT_NODE, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            return zooKeeper;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
