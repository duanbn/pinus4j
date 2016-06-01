package org.pinus4j.api;

import javax.annotation.Resource;
import javax.sql.DataSource;

import junit.framework.Assert;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:applicationContext.xml" })
public class SpringPinusClientTest {

    @Resource
    private ApplicationContext appCtx;

    @Resource
    private PinusClient        pinusClient;

    @Test
    public void testInjectDaaSource() throws Exception {
        DataSource pinusGlobal = (DataSource) appCtx.getBean("pinus-global");
        Assert.assertNotNull(pinusGlobal);
        Assert.assertFalse(pinusGlobal.getConnection().isClosed());

        DataSource pinusSharding1 = (DataSource) appCtx.getBean("pinus-sharding1");
        Assert.assertFalse(pinusSharding1.getConnection().isClosed());

        DataSource pinusSharding2 = (DataSource) appCtx.getBean("pinus-sharding2");
        Assert.assertNotNull(pinusSharding2);
        Assert.assertFalse(pinusSharding2.getConnection().isClosed());

    }

}
