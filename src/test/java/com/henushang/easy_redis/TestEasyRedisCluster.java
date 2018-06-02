package com.henushang.easy_redis;

import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TestEasyRedisCluster {

    private static String host = "127.0.0.1:6379,127.0.0.1:1111,127.0.0.1:2222";

    @Ignore
    @Test
    public void testLpush() {
        EasyRedisCluster cluster = new EasyRedisCluster(host);

        int dataNum = 10000;
        for (int i = 0; i < dataNum; i++) {
            cluster.lpush("key1", String.valueOf(i));
        }

        int totalNum = 0;
        for (int i = 0; i < 10000; i++) {
            List<String> values = cluster.rpop("key1", 1);
            if (values == null) {
                break;
            }
            totalNum += values.size();
        }
        Assert.assertEquals(dataNum, totalNum);
        totalNum = 0;

        for (int i = 0; i < dataNum; i++) {
            cluster.rpush("key2", String.valueOf(i));
        }

        for (int i = 0; i < 10000; i++) {
            List<String> values = cluster.lpop("key2", 1);
            if (values == null) {
                break;
            }
            totalNum += values.size();
        }
        Assert.assertEquals(dataNum, totalNum);
    }

}
