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
package org.pinus4j.cache;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Map;

/**
 * database cache interface.
 *
 * @author duanbn
 * @since 0.7.1
 */
public interface ICache {

    /**
     * 获取缓存客户端，不同的缓存会有不同的实现<br/>
     * <b>expert</b>
     * 
     * @return
     */
    public Object getCacheClient();

    /**
     * 初始化缓存
     */
    public void init();

    /**
     * 销毁对象
     */
    public void close();

    /**
     * 获取可以用的服务链接.
     */
    public Collection<SocketAddress> getAvailableServers();

    /**
     * 获取过期时间
     * 
     * @return
     */
    public int getExpire();

    /**
     * 设置缓存属性. 属性值来自<primary>和<second>节点的属性
     * 
     * @param properties
     */
    public void setProperties(Map<String, String> properties);

    /**
     * 获取缓存属性
     * 
     * @return
     */
    public Map<String, String> getProperties();

}
