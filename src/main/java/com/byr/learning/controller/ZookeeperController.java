package com.byr.learning.controller;

import com.baomidou.mybatisplus.extension.api.ApiResult;
import com.byr.learning.config.CuratorConfiguration;
import com.byr.learning.service.ZooKeeperLockService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.TimeUnit;

/**
 * Distributed lock controller controller
 * <p/>
 * Created in 2018.11.12
 * <p/>
 *
 * @author Liaozihong
 */
@RestController
@Slf4j
@Api(value = "分布式锁测试接口", tags = "DistributedLockTestApi")
public class ZookeeperController {
    static Integer number = 0;
    static Integer count = 0;
    /**
     * 锁超时时间
     */
    static Integer TIME_OUT = 3;
    @Autowired
    ZooKeeperLockService zooKeeperLockService;
    @Autowired
    CuratorConfiguration curatorConfiguration;
    @Autowired
    CuratorFramework curatorFramework;
    private final static String ROOT_LOCK_NODE = "/byr";

    /**
     * 分布式可重入排它锁
     *
     * @return the lock 1
     */
    @GetMapping("/processMutex")
    public ApiResult processMutex() throws Exception {
        log.info("第{}个线程请求锁", ++count);
        try {
            InterProcessMutex mutex = new InterProcessMutex(curatorFramework, ROOT_LOCK_NODE);
            if (mutex.acquire(TIME_OUT, TimeUnit.SECONDS)) {
                try {
                    log.info("获得锁线程为：{}", ++number);
                    return ApiResult.ok("加锁成功");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    log.info("解锁");
                    mutex.release();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ApiResult.failed("加锁失败");
    }

    /**
     * 分布式排它锁
     *
     * @return the lock 1
     */
    @GetMapping("/semaphoreMutex")
    public ApiResult semaphoreMutex() throws Exception {
        log.info("第{}个线程请求锁", ++count);
        try {
            InterProcessSemaphoreMutex mutex = new InterProcessSemaphoreMutex(curatorFramework, ROOT_LOCK_NODE);
            if (mutex.acquire(TIME_OUT, TimeUnit.SECONDS)) {
                try {
                    log.info("获得锁线程为：{}", ++number);
                    return ApiResult.ok("加锁成功");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    log.info("解锁");
                    mutex.release();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ApiResult.failed("加锁失败");
    }

    /**
     * 分布式读写锁
     *
     * @return the lock 1
     */
    @GetMapping("/readWriteLock")
    public ApiResult readWriteLock() throws Exception {
        log.info("第{}个线程请求锁", ++count);
        try {
            InterProcessReadWriteLock readWriteLock = new InterProcessReadWriteLock(curatorFramework, ROOT_LOCK_NODE);
            //读锁
            InterProcessMutex readLock = readWriteLock.readLock();
            //写锁
            InterProcessMutex writeLock = readWriteLock.writeLock();
            try {
                //注意只有先得到写锁在得到读锁，不能反过来
                if (!writeLock.acquire(TIME_OUT, TimeUnit.SECONDS)) {
                    return ApiResult.failed("得到写锁失败");
                }
                log.info("已经得到写锁");
                if (!readLock.acquire(TIME_OUT, TimeUnit.SECONDS)) {
                    return ApiResult.failed("得到写锁失败");
                }
                log.info("已经得到读锁");
                log.info("获得锁线程数为：{}", ++number);
                return ApiResult.ok("加锁成功");
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                log.info("解锁");
                writeLock.release();
                readLock.release();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets lock 1.
     *
     * @return the lock 1
     */
    @GetMapping("/zookeeper_lock")
    @ApiOperation(value = "zookeeper原生实现分布式阻塞锁", notes = "获取分布式锁，不能加锁则等待上一个锁的释放", response = ApiResult.class)
    public ApiResult getLock3() throws Exception {
        try {
            if (zooKeeperLockService.lock(ROOT_LOCK_NODE, "lock")) {
                try {
                    Thread.sleep(5000);
                    return ApiResult.ok("加锁成功");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    zooKeeperLockService.unlock();
                }
            }
            return ApiResult.failed("加锁失败");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}