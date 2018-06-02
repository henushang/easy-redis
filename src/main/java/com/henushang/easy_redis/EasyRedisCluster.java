package com.henushang.easy_redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

/**
 *
 * 本类的初始化方法为：EasyRedisCluster easyCluster = new EasyRedisCluster("127.0.0.1:1111,127.0.0.1:2222");
 * <br />
 * 内部使用ShardedJedisPool来管理连接，暂不对外提供修改接口，使用默认的配置。
 *
 */
public class EasyRedisCluster {

    private ShardedJedisPool shardedJedisPool = null;

    /**
     * 类的实例化方式，支持使用字符串类型来描述具体的 redis 的 ip:port
     * @param addressInfo ip1:port1,ip2:port2
     */
    public EasyRedisCluster(String addressInfo) {
        initPool(getJedisShardInfoList(addressInfo));
    }

    /**
     * 随机选择一个Redis实例，把数据放进去
     * @param key
     * @param values
     * @return
     */
    public boolean lpush(String key, String... values) {
        ShardedJedis shardedJedis = getShardedJedis();
        Jedis jedis = getRandomJedis(shardedJedis);
        Long re = jedis.lpush(key, values);
        shardedJedis.close();
        return re > 0;
    }

    /**
     * 随机选择一个Redis实例，把数据放进去
     * @param key
     * @param values
     * @return
     */
    public boolean rpush(String key, String... values) {
        ShardedJedis shardedJedis = getShardedJedis();
        Jedis jedis = getRandomJedis(shardedJedis);
        Long re = jedis.rpush(key, values);
        shardedJedis.close();
        return re > 0;
    }

    /**
     * 从多个redis分片中取数据
     * @param key
     * @param numPerShard 每一个分片中单次取出的数据量
     * @return
     */
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

    /**
     * 从多个redis分片中取数据
     * @param key
     * @param numPerShard 每一个分片中单次取出的数据量
     * @return
     */
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
