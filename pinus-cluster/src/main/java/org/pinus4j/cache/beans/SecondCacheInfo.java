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

package org.pinus4j.cache.beans;

import java.util.Map;

import org.pinus4j.cache.ISecondCache;

/**
 * 二级缓存信息
 * 
 * @author shanwei Jul 6, 2015 7:14:08 PM
 */
public class SecondCacheInfo {

    private Class<ISecondCache> secondCacheClass;
    private int                 secondCacheExpire;
    private String              secondCacheAddress;
    private Map<String, String> secondCacheAttr;

    @Override
    public String toString() {
        return "SecondCacheInfo [secondCacheClass=" + secondCacheClass + ", secondCacheExpire=" + secondCacheExpire
                + ", secondCacheAddress=" + secondCacheAddress + ", secondCacheAttr=" + secondCacheAttr + "]";
    }

    public Class<ISecondCache> getSecondCacheClass() {
        return secondCacheClass;
    }

    public void setSecondCacheClass(Class<ISecondCache> secondCacheClass) {
        this.secondCacheClass = secondCacheClass;
    }

    public int getSecondCacheExpire() {
        return secondCacheExpire;
    }

    public void setSecondCacheExpire(int secondCacheExpire) {
        this.secondCacheExpire = secondCacheExpire;
    }

    public String getSecondCacheAddress() {
        return secondCacheAddress;
    }

    public void setSecondCacheAddress(String secondCacheAddress) {
        this.secondCacheAddress = secondCacheAddress;
    }

    public Map<String, String> getSecondCacheAttr() {
        return secondCacheAttr;
    }

    public void setSecondCacheAttr(Map<String, String> secondCacheAttr) {
        this.secondCacheAttr = secondCacheAttr;
    }

}
