package com.henushang.easy_redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class EasyRedisCluster {

    private ShardedJedisPool shardedJedisPool = null;

    public EasyRedisCluster(String addressInfo) {
        initPool(getJedisShardInfoList(addressInfo));
    }

    public boolean lpush(String key, String... values) {
        ShardedJedis shardedJedis = getShardedJedis();
        Jedis jedis = getRandomJedis(shardedJedis);
        Long re = jedis.lpush(key, values);
        shardedJedis.close();
        return re > 0;
    }

    public boolean rpush(String key, String... values) {
        ShardedJedis shardedJedis = getShardedJedis();
        Jedis jedis = getRandomJedis(shardedJedis);
        Long re = jedis.rpush(key, values);
        shardedJedis.close();
        return re > 0;
    }

    public List<String> lpop(String key, Integer numPerShard) {
        ShardedJedis shardedJedis = getShardedJedis();
        List<Jedis> allShard = new ArrayList<Jedis>(shardedJedis.getAllShards());

        List<String> result = new ArrayList<String>();
        for (Jedis jedis : allShard) {
            for (int i = 0; i < numPerShard; i++) {
                String value = jedis.lpop(key);
                if (value == null) {
                    break;
                }
                result.add(value);
            }
        }
        shardedJedis.close();
        return result;
    }

    public List<String> rpop(String key, Integer numPerShard) {
        ShardedJedis shardedJedis = getShardedJedis();
        List<Jedis> allShard = new ArrayList<Jedis>(shardedJedis.getAllShards());

        List<String> result = new ArrayList<String>();
        for (Jedis jedis : allShard) {
            for (int i = 0; i < numPerShard; i++) {
                String value = jedis.rpop(key);
                if (value == null) {
                    break;
                }
                result.add(value);
            }
        }
        shardedJedis.close();
        return result;
    }

    private ShardedJedis getShardedJedis() {
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        return shardedJedis;
    }

    private Jedis getRandomJedis(ShardedJedis shardedJedis) {
        Jedis randomJedis = null;
        List<Jedis> allShards = new ArrayList<Jedis>(shardedJedis.getAllShards());
        int shardNum = allShards.size();
        if (shardNum == 1) {
            randomJedis = allShards.get(0);
        } else {
            Random r = new Random();
            int randomIndex = r.nextInt(shardNum);
            randomJedis = allShards.get(randomIndex);
        }

        return randomJedis;
    }

    private void initPool(List<JedisShardInfo> jedisShardInfoList) {
        JedisPoolConfig config = new JedisPoolConfig();
        shardedJedisPool = new ShardedJedisPool(config, jedisShardInfoList);
    }

    private List<JedisShardInfo> getJedisShardInfoList(String addressInfo) {
        String[] addressArray = addressInfo.split(",");
        List<JedisShardInfo> jedisShardInfoList = new ArrayList<JedisShardInfo>(
                addressArray.length);
        for (String address : addressArray) {
            String[] hostPort = address.split(":");
            JedisShardInfo shardInfo = new JedisShardInfo(hostPort[0],
                    Integer.parseInt(hostPort[1]));
            jedisShardInfoList.add(shardInfo);
        }
        return jedisShardInfoList;
    }
}
