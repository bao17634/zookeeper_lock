package com.byr.learning.service;

public interface ZooKeeperLockService {
    /**
     * 加锁，不能加锁则等待上一个锁的释放
     * @param rootLockNode 锁路径
     * @param lockName 锁名
     * @return
     */
    boolean lock(String rootLockNode,String lockName);

    /**
     * 加锁，获取不到锁返回
     * @param rootLockNode 锁路径
     * @param lockName 锁名
     * @return
     */
    boolean tryLock(String rootLockNode,String lockName);

    /**
     * 解锁
     * @return
     */
    void unLock(String rootLockNode) throws InterruptedException;
}
