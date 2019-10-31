package com.byr.learning.config;

/**
 * @author: byr
 */

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CuratorConfiguration {
    @Autowired
    ConfigProperties configProperties;


    @Bean(initMethod = "start")
    public CuratorFramework curatorFramework() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(configProperties.getElapsedTimeMs(), configProperties.getRetryCount());
        return CuratorFrameworkFactory.newClient(
                configProperties.getConnectString(),
                configProperties.getSessionTimeoutMs(),
                configProperties.getConnectionTimeoutMs(),
                retryPolicy);
    }
}
