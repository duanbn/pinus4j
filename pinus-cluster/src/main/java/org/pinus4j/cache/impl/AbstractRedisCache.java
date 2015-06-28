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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;

import com.google.common.collect.Lists;

public abstract class AbstractRedisCache extends AbstractCache {

    public static final Logger LOG = LoggerFactory.getLogger(AbstractRedisCache.class);

    protected ShardedJedis     redisClient;

    public AbstractRedisCache(String address, int expire) {
        super(address, expire);

        try {
            String[] addresses = address.split(",");

            List<JedisShardInfo> shardInfos = Lists.newArrayListWithCapacity(addresses.length);
            for (String addr : addresses) {
                String[] pair = addr.split(":");
                shardInfos.add(new JedisShardInfo(pair[0], Integer.parseInt(pair[1])));
            }

            this.redisClient = new ShardedJedis(shardInfos);
        } catch (Exception e) {
            throw new RuntimeException("connect redis server failure", e);
        }
    }
    
    @Override
    public Collection<SocketAddress> getAvailableServers() {
        List<SocketAddress> servers = Lists.newArrayList();

        Collection<Jedis> alives = redisClient.getAllShards();
        for (Jedis alive : alives) {
            String host = alive.getClient().getHost();
            int port = alive.getClient().getPort();
            servers.add(new InetSocketAddress(host, port));
        }

        return servers;
    }

    @Override
    public void close() {
        this.redisClient.close();
    }

}
