/**
 * Copyright 2014 Duan Bingnan
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pinus4j.cache.impl;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import com.google.common.collect.Lists;

public abstract class AbstractRedisCache extends AbstractCache {

    public static final Logger LOG = LoggerFactory.getLogger(AbstractRedisCache.class);

    protected ShardedJedisPool jedisPool;

    public AbstractRedisCache(String address, int expire) {
        super(address, expire);
    }

    @Override
    public Object getCacheClient() {
        return this.jedisPool;
    }

    @Override
    public void init() {
        try {
            String[] addresses = address.split(",");

            List<JedisShardInfo> shardInfos = Lists.newArrayListWithCapacity(addresses.length);
            JedisShardInfo shardInfo = null;
            for (String addr : addresses) {

                int firstSplitPos = addr.indexOf(':');
                int secondSplitPos = addr.indexOf(':', firstSplitPos + 1);

                String host = addr.substring(0, firstSplitPos);
                if (secondSplitPos == -1) {
                    int port = Integer.parseInt(addr.substring(firstSplitPos + 1));
                    shardInfo = new JedisShardInfo(host, port);
                } else {
                    int port = Integer.parseInt(addr.substring(firstSplitPos + 1, secondSplitPos));
                    String pwd = addr.substring(secondSplitPos + 1);
                    shardInfo = new JedisShardInfo(host, port);
                    shardInfo.setPassword(pwd);
                }

                shardInfos.add(shardInfo);
            }

            Map<String, String> properties = getProperties();

            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setTestWhileIdle(true);
            poolConfig.setMaxIdle(6);
            if (properties.containsKey("maxIdle")) {
                poolConfig.setMaxIdle(Integer.parseInt(properties.get("maxIdle")));
            }
            poolConfig.setMaxTotal(2000);
            if (properties.containsKey("maxTotal")) {
                poolConfig.setMaxTotal(Integer.parseInt(properties.get("maxTotal")));
            }
            poolConfig.setMinEvictableIdleTimeMillis(60000);
            if (properties.containsKey("minEvictableIdleTimeMillis")) {
                poolConfig.setMinEvictableIdleTimeMillis(Long.parseLong("minEvictableIdleTimeMillis"));
            }
            poolConfig.setTimeBetweenEvictionRunsMillis(30000);
            if (properties.containsKey("timeBetweenEvictionRunsMillis")) {
                poolConfig.setTimeBetweenEvictionRunsMillis(Long.parseLong("timeBetweenEvictionRunsMillis"));
            }
            poolConfig.setNumTestsPerEvictionRun(-1);

            this.jedisPool = new ShardedJedisPool(poolConfig, shardInfos);
        } catch (Exception e) {
            throw new RuntimeException("connect redis server failure", e);
        }
    }

    @Override
    public void close() {
        //        this.redisClient.close();
        this.jedisPool.close();
    }

    @Override
    public Collection<SocketAddress> getAvailableServers() {
        List<SocketAddress> servers = Lists.newArrayList();

        ShardedJedis redisClient = null;
        try {
            redisClient = jedisPool.getResource();
            Collection<Jedis> alives = redisClient.getAllShards();
            for (Jedis alive : alives) {
                String host = alive.getClient().getHost();
                int port = alive.getClient().getPort();
                servers.add(new InetSocketAddress(host, port));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (redisClient != null) {
                redisClient.close();
            }
        }

        return servers;
    }

}
