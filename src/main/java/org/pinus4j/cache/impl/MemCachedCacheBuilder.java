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
import org.pinus4j.cluster.config.IClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * cache builder for memcached.
 *
 * @author duanbn
 * @since 0.7.1
 */
public class MemCachedCacheBuilder implements ICacheBuilder {

	public static final Logger LOG = LoggerFactory.getLogger(MemCachedCacheBuilder.class);

	private boolean isCacheEnabled;

	private Class<IPrimaryCache> primaryCacheClass;

	private String primaryCacheAddress;

	private int primaryCacheExpire;

	private Class<ISecondCache> secondCacheClass;

	private String secondCacheAddress;

	private int secondCacheExpire;

	private MemCachedCacheBuilder() {
	}

	public static ICacheBuilder valueOf(IClusterConfig config) {
		MemCachedCacheBuilder builder = new MemCachedCacheBuilder();

		builder.setCacheEnabled(config.isCacheEnabled());

		builder.setPrimaryCacheClass(config.getPrimaryCacheClass());
		builder.setPrimaryCacheAddress(config.getPrimaryCacheAddress());
		builder.setPrimaryCacheExpire(config.getPrimaryCacheExpire());

		builder.setSecondCacheClass(config.getSecondCacheClass());
		builder.setSecondCacheAddress(config.getSecondCacheAddress());
		builder.setSecondCacheExpire(config.getSecondCacheExpire());

		return builder;
	}

	/**
	 * build primary cache.
	 */
	public IPrimaryCache buildPrimaryCache() {
		if (!this.isCacheEnabled) {
			return null;
		}

		Constructor<IPrimaryCache> c;
		IPrimaryCache instance = null;
		try {
			c = this.primaryCacheClass.getDeclaredConstructor(String.class, Integer.TYPE);
			instance = c.newInstance(this.primaryCacheAddress, this.primaryCacheExpire);
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
			LOG.info("find primary cache, expire " + this.primaryCacheExpire + " seconds, memcached server - "
					+ memcachedAddressInfo.toString());
		}

		return instance;
	}

	/**
	 * build second cache.
	 */
	public ISecondCache buildSecondCache() {
		if (!this.isCacheEnabled) {
			return null;
		}

		Constructor<ISecondCache> c;
		ISecondCache instance = null;
		try {
			c = this.secondCacheClass.getDeclaredConstructor(String.class, Integer.TYPE);
			instance = c.newInstance(this.secondCacheAddress, this.secondCacheExpire);
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
			LOG.info("find second cache, expire " + this.secondCacheExpire + " seconds, memcached server - "
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

	public void setPrimaryCacheClass(Class<IPrimaryCache> clazz) {
		this.primaryCacheClass = clazz;
	}

	public Class<IPrimaryCache> getPrimaryCacheClass() {
		return this.primaryCacheClass;
	}

	public void setSecondCacheClass(Class<ISecondCache> clazz) {
		this.secondCacheClass = clazz;
	}

	public Class<ISecondCache> getSecondCacheClass() {
		return this.secondCacheClass;
	}

	public String getPrimaryCacheAddress() {
		return primaryCacheAddress;
	}

	public void setPrimaryCacheAddress(String primaryCacheAddress) {
		this.primaryCacheAddress = primaryCacheAddress;
	}

	public int getPrimaryCacheExpire() {
		return primaryCacheExpire;
	}

	public void setPrimaryCacheExpire(int primaryCacheExpire) {
		this.primaryCacheExpire = primaryCacheExpire;
	}

	public String getSecondCacheAddress() {
		return secondCacheAddress;
	}

	public void setSecondCacheAddress(String secondCacheAddress) {
		this.secondCacheAddress = secondCacheAddress;
	}

	public int getSecondCacheExpire() {
		return secondCacheExpire;
	}

	public void setSecondCacheExpire(int secondCacheExpire) {
		this.secondCacheExpire = secondCacheExpire;
	}

	public boolean isCacheEnabled() {
		return isCacheEnabled;
	}

	public void setCacheEnabled(boolean isCacheEnabled) {
		this.isCacheEnabled = isCacheEnabled;
	}
}
