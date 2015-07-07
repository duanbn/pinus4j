package org.pinus4j.cache.beans;

import java.util.Map;

import org.pinus4j.cache.IPrimaryCache;

/**
 * 一级缓存配置信息
 * 
 * @author shanwei Jul 6, 2015 7:12:46 PM
 */
public class PrimaryCacheInfo {

    private Class<IPrimaryCache> primaryCacheClass;
    private int                  primaryCacheExpire;
    private String               primaryCacheAddress;
    private Map<String, String>  primaryCacheAttr;

    @Override
    public String toString() {
        return "PrimaryCacheInfo [primaryCacheClass=" + primaryCacheClass + ", primaryCacheExpire="
                + primaryCacheExpire + ", primaryCacheAddress=" + primaryCacheAddress + ", primaryCacheAttr="
                + primaryCacheAttr + "]";
    }

    public Class<IPrimaryCache> getPrimaryCacheClass() {
        return primaryCacheClass;
    }

    public void setPrimaryCacheClass(Class<IPrimaryCache> primaryCacheClass) {
        this.primaryCacheClass = primaryCacheClass;
    }

    public int getPrimaryCacheExpire() {
        return primaryCacheExpire;
    }

    public void setPrimaryCacheExpire(int primaryCacheExpire) {
        this.primaryCacheExpire = primaryCacheExpire;
    }

    public String getPrimaryCacheAddress() {
        return primaryCacheAddress;
    }

    public void setPrimaryCacheAddress(String primaryCacheAddress) {
        this.primaryCacheAddress = primaryCacheAddress;
    }

    public Map<String, String> getPrimaryCacheAttr() {
        return primaryCacheAttr;
    }

    public void setPrimaryCacheAttr(Map<String, String> primaryCacheAttr) {
        this.primaryCacheAttr = primaryCacheAttr;
    }

}
