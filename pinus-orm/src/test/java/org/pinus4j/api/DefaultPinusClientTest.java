package org.pinus4j.api;

import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.impl.Condition;
import org.pinus4j.entity.TestGlobalEntity;

public class DefaultPinusClientTest extends BaseTest {

    private static PinusClient pinusClient;

    @BeforeClass
    public static void beforeClass() {
        pinusClient = getPinusClient();
    }

    @AfterClass
    public static void afterClass() {
        pinusClient.destroy();
    }

    @Test
    public void testSave() throws Exception {
        TestGlobalEntity entity = createGlobalEntity();
        pinusClient.save(entity);
    }

    @Test
    public void testQuery() throws Exception {
        IQuery query = pinusClient.createQuery(TestGlobalEntity.class);
        query.and(Condition.eq("testInt", 5));
        List<TestGlobalEntity> list = query.list();
        for (TestGlobalEntity entity : list) {
            System.out.println(entity);
        }
    }

}
