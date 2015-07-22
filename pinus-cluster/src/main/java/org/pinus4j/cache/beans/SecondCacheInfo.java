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
