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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.spy.memcached.MemcachedClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * base cache implmenets.
 *
 * @author duanbn
 * @since 0.7.1
 */
public abstract class AbstractMemCachedCache extends AbstractCache {

    public static final Logger LOG = LoggerFactory.getLogger(AbstractMemCachedCache.class);

    protected MemcachedClient  memClient;

    public AbstractMemCachedCache(String address, int expire) {
        super(address, expire);
    }

    @Override
    public Object getCacheClient() {
        return this.memClient;
    }

    @Override
    public void init() {
        try {
            List<InetSocketAddress> servers = new ArrayList<InetSocketAddress>();

            String[] addresses = address.split(",");

            InetSocketAddress socketAddress = null;
            for (String addr : addresses) {
                String[] pair = addr.split(":");
                socketAddress = new InetSocketAddress(pair[0], Integer.parseInt(pair[1]));
                servers.add(socketAddress);
            }

            this.memClient = new MemcachedClient(servers);
        } catch (Exception e) {
            throw new RuntimeException("连接memcached服务器失败", e);
        }
    }

    /**
     * 销毁对象
     */
    @Override
    public void close() {
        this.memClient.shutdown();
    }

    @Override
    public Collection<SocketAddress> getAvailableServers() {
        if (this.memClient == null) {
            return null;
        }
        return this.memClient.getAvailableServers();
    }

}
