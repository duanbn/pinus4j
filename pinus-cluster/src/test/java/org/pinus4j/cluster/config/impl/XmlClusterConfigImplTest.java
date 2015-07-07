package org.pinus4j.cluster.config.impl;

import org.junit.BeforeClass;
import org.junit.Test;
import org.pinus4j.cache.beans.PrimaryCacheInfo;
import org.pinus4j.cache.beans.SecondCacheInfo;
import org.pinus4j.cluster.config.IClusterConfig;
import org.pinus4j.exceptions.LoadConfigException;

public class XmlClusterConfigImplTest {

    private static IClusterConfig config;

    @BeforeClass
    public static void beforeClass() throws LoadConfigException {
        config = XmlClusterConfigImpl.getInstance();
    }

    @Test
    public void testGetPrimaryCacheInfo() {
        PrimaryCacheInfo primaryCacheInfo = config.getPrimaryCacheInfo();
        System.out.println(primaryCacheInfo);
    }

    @Test
    public void testGetSecondCacheInfo() {
        SecondCacheInfo secondCacheInfo = config.getSecondCacheInfo();
        System.out.println(secondCacheInfo);
    }

}
