package com.byr.learning.service.impl;

import com.byr.learning.service.ZooKeeperLockService;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class ZooKeeperLockServiceImpl implements ZooKeeperLockService {
    /**
     * 锁超时间
     */
    final Integer TIME_OUT = 3000;
    /**
     * 当前锁节点
     */
    private String currentLock;
    /**
     * 等待的锁（前一个锁）
     */
    private String waitLock;
    /**
     * 计数器（用来在加锁失败时阻塞加锁线程）
     */
    private CountDownLatch countDownLatch;
    @Autowired
    ZooKeeper zooKeeper;

    @Override
    public boolean lock(String rootLockNode, String lockName) {
        if (tryLock(rootLockNode, lockName)) {
            log.info("线程{}加锁{}成功", Thread.currentThread().getName(), this.currentLock);
            return true;
        } else {
            return waitOtherLock(rootLockNode, this.waitLock, TIME_OUT);
        }
    }

    @Override
    public boolean tryLock(String rootLockNode, String lockName) {
        // 分隔符
        String split = "_lock_";
        if (lockName.contains(split)) {
            throw new RuntimeException("lockName can't contains '_lock_' ");
        }
        try {
            String nodePath = rootLockNode + "/" + lockName + split;
            Stat stat = zooKeeper.exists(nodePath, false);
            if (null == stat) {
                // 创建锁节点（临时有序节点）
                String rootChildNode = zooKeeper.create(nodePath, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                this.currentLock = rootChildNode.substring(rootChildNode.lastIndexOf("/"), rootChildNode.length());
                log.info("线程{}创建锁节点{}成功", Thread.currentThread().getName(), this.currentLock);
            } else {
                return false;
            }
            // 获取根节点所有子节点
            List<String> nodes = zooKeeper.getChildren(rootLockNode, false);
            // 取所有竞争lockName的锁
            List<String> lockNodes = new ArrayList<String>();
            for (String nodeName : nodes) {
                if (nodeName.split(split)[0].equals(lockName)) {
                    lockNodes.add(nodeName);
                }
            }
            Collections.sort(lockNodes);
            // 取最小节点与当前锁节点比对加锁
            String currentLockPath = "/" + lockNodes.get(0);
            if (this.currentLock.equals(currentLockPath)) {
                return true;
            }
            // 加锁失败，设置前一节点为等待锁节点
            String currentLockNode = this.currentLock;
            String localNode = currentLockNode.replace("/", "");
            int preNodeIndex = Collections.binarySearch(lockNodes, localNode) - 1;
            if (preNodeIndex < 0) {
                return false;
            }
            this.waitLock = lockNodes.get(preNodeIndex);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    private boolean waitOtherLock(String rootLockNode, String waitLock, int sessionTimeout) {
        boolean islock = false;
        try {
            // 监听等待锁节点
            String waitLockNode = rootLockNode + "/" + waitLock;
            Stat stat = zooKeeper.exists(waitLockNode, true);
            if (null != stat) {
                log.info("线程{}锁{}加锁失败，等待锁{}释放", Thread.currentThread().getName(), this.currentLock, waitLock);
                // 设置计数器，使用计数器阻塞线程
                this.countDownLatch = new CountDownLatch(1);
                islock = this.countDownLatch.await(sessionTimeout, TimeUnit.MILLISECONDS);
                this.countDownLatch = null;
                if (islock) {
                    log.info("线程:{}锁:{}加锁成功，锁:{}已经释放", Thread.currentThread().getName(), this.currentLock, waitLock);
                } else {
                    log.info("线程:{}锁:{}加锁失败", Thread.currentThread().getName(), this.currentLock);
                }
            } else {
                islock = true;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return islock;
    }

    @Override
    public void unLock(String rootLockNode) throws InterruptedException {
        try {
            String nodePath = rootLockNode + this.currentLock;
            Stat stat = zooKeeper.exists(nodePath, false);
            if (null != stat) {
                log.info("线程:{}释放锁:{}", Thread.currentThread().getName(), this.currentLock);
                zooKeeper.delete(nodePath, -1);
                this.currentLock = null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
//            zooKeeper.close();
        }
    }

}
