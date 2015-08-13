package org.pinus4j.api;

import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.impl.Order;
import org.pinus4j.entity.TestEntity;

import com.google.common.collect.Lists;

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
        List<TestEntity> testEntityList = Lists.newArrayList();
        for (int i = 0; i < 17; i++) {
            testEntityList.add(createEntity());
        }

        pinusClient.beginTransaction();
        try {
            pinusClient.saveBatch(testEntityList, false);
            pinusClient.commit();
        } catch (Exception e) {
            e.printStackTrace();
            pinusClient.rollback();
        }
    }

    @Test
    public void testDelete() throws Exception {
        IQuery<TestEntity> query = pinusClient.createQuery(TestEntity.class);
        List<TestEntity> testEntityList = query.list();

        try {
            pinusClient.beginTransaction();
            pinusClient.delete(testEntityList);
            pinusClient.commit();
        } catch (Exception e) {
            pinusClient.rollback();
        }
    }

    @Test
    public void testQuery() throws Exception {
        IQuery<TestEntity> query = pinusClient.createQuery(TestEntity.class);
        query.orderBy("testInt", Order.DESC);
        List<TestEntity> testEntityList = query.list();

        for (TestEntity testEntity : testEntityList) {
            System.out.println(testEntity);
        }

        System.out.println(testEntityList.size());
    }

}
