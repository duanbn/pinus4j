package org.pinus4j.cache.impl;

import org.pinus4j.cache.ICache;

/**
 * @author bingnan.dbn Jun 25, 2015 3:33:10 PM
 */
public abstract class AbstractCache implements ICache {

    protected int expire = 30;

    public AbstractCache(String address, int expire) {
        if (expire > 0) {
            this.expire = expire;
        }
    }

    @Override
    public int getExpire() {
        return this.expire;
    }

}
