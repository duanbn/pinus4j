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

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;

import org.pinus4j.cache.ICacheBuilder;
import org.pinus4j.cache.IPrimaryCache;
import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cache.beans.PrimaryCacheInfo;
import org.pinus4j.cache.beans.SecondCacheInfo;
import org.pinus4j.cluster.config.IClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * default cache builder.
 *
 * @author duanbn
 * @since 0.7.1
 */
public class DefaultCacheBuilder implements ICacheBuilder {

    public static final Logger LOG = LoggerFactory.getLogger(DefaultCacheBuilder.class);

    private boolean            isCacheEnabled;

    private PrimaryCacheInfo   primaryCacheInfo;

    private SecondCacheInfo    secondCacheInfo;

    private DefaultCacheBuilder() {
    }

    public static ICacheBuilder valueOf(IClusterConfig config) {
        DefaultCacheBuilder builder = new DefaultCacheBuilder();

        builder.setCacheEnabled(config.isCacheEnabled());

        builder.setPrimaryCacheInfo(config.getPrimaryCacheInfo());
        builder.setSecondCacheInfo(config.getSecondCacheInfo());

        return builder;
    }

    /**
     * build primary cache.
     */
    public IPrimaryCache buildPrimaryCache() {
        if (!this.isCacheEnabled || this.primaryCacheInfo == null) {
            return null;
        }

        Constructor<IPrimaryCache> c;
        IPrimaryCache instance = null;
        try {
            Class<IPrimaryCache> primaryCacheClass = primaryCacheInfo.getPrimaryCacheClass();
            c = primaryCacheClass.getDeclaredConstructor(String.class, Integer.TYPE);

            instance = c.newInstance(primaryCacheInfo.getPrimaryCacheAddress(),
                    primaryCacheInfo.getPrimaryCacheExpire());
            instance.setProperties(primaryCacheInfo.getPrimaryCacheAttr());
            instance.init();
        } catch (Exception e) {
            throw new RuntimeException("create primary cache instance failure", e);
        }

        _sleep(100);

        StringBuilder memcachedAddressInfo = new StringBuilder();
        Collection<SocketAddress> servers = instance.getAvailableServers();
        if (servers != null && !servers.isEmpty()) {
            for (SocketAddress server : servers) {
                memcachedAddressInfo.append(((InetSocketAddress) server).getAddress().getHostAddress() + ":"
                        + ((InetSocketAddress) server).getPort());
                memcachedAddressInfo.append(",");
            }
            memcachedAddressInfo.deleteCharAt(memcachedAddressInfo.length() - 1);
            LOG.info("find primary cache[" + this.primaryCacheInfo.getPrimaryCacheClass() + "], expire "
                    + this.primaryCacheInfo.getPrimaryCacheExpire() + " seconds, cache server - "
                    + memcachedAddressInfo.toString());
        }

        return instance;
    }

    /**
     * build second cache.
     */
    public ISecondCache buildSecondCache() {
        if (!this.isCacheEnabled || this.secondCacheInfo == null) {
            return null;
        }

        Constructor<ISecondCache> c;
        ISecondCache instance = null;
        try {
            Class<ISecondCache> secondCacheClass = secondCacheInfo.getSecondCacheClass();
            c = secondCacheClass.getDeclaredConstructor(String.class, Integer.TYPE);

            instance = c.newInstance(this.secondCacheInfo.getSecondCacheAddress(),
                    this.secondCacheInfo.getSecondCacheExpire());
            instance.setProperties(this.secondCacheInfo.getSecondCacheAttr());
            instance.init();
        } catch (Exception e) {
            throw new RuntimeException("create second cache instance failure", e);
        }

        _sleep(100);

        StringBuilder memcachedAddressInfo = new StringBuilder();
        Collection<SocketAddress> servers = instance.getAvailableServers();
        if (servers != null && !servers.isEmpty()) {
            for (SocketAddress server : servers) {
                memcachedAddressInfo.append(((InetSocketAddress) server).getAddress().getHostAddress() + ":"
                        + ((InetSocketAddress) server).getPort());
                memcachedAddressInfo.append(",");
            }
            memcachedAddressInfo.deleteCharAt(memcachedAddressInfo.length() - 1);
            LOG.info("find second cache[" + this.secondCacheInfo.getSecondCacheClass() + "], expire "
                    + this.secondCacheInfo.getSecondCacheExpire() + " seconds, cache server - "
                    + memcachedAddressInfo.toString());
        }

        return instance;
    }

    private void _sleep(int time) {
        try {
            Thread.sleep(time);
        } catch (Exception e) {
        }
    }

    public PrimaryCacheInfo getPrimaryCacheInfo() {
        return primaryCacheInfo;
    }

    public void setPrimaryCacheInfo(PrimaryCacheInfo primaryCacheInfo) {
        this.primaryCacheInfo = primaryCacheInfo;
    }

    public SecondCacheInfo getSecondCacheInfo() {
        return secondCacheInfo;
    }

    public void setSecondCacheInfo(SecondCacheInfo secondCacheInfo) {
        this.secondCacheInfo = secondCacheInfo;
    }

    public boolean isCacheEnabled() {
        return isCacheEnabled;
    }

    public void setCacheEnabled(boolean isCacheEnabled) {
        this.isCacheEnabled = isCacheEnabled;
    }
}
