package org.pinus4j.api;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.impl.Condition;
import org.pinus4j.api.query.impl.Order;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.entity.TestGlobalEntity;
import org.pinus4j.exceptions.DBOperationException;

import com.google.common.collect.Lists;

public class DefaultPinusClientTest extends BaseTest {

    private static TestGlobalEntity       globalEntity;
    private static List<TestGlobalEntity> globalEntities;

    private static TestEntity             shardingEntity1;
    private static TestEntity             shardingEntity2;
    private static List<TestEntity>       shardingEntities;

    /**
     * create data.
     */
    @BeforeClass
    public static void before() {
        // save and update global one
        globalEntity = createGlobalEntity();
        globalEntity.setoTestLong(8l);
        pinusClient.save(globalEntity);
        globalEntity.setTestString("i am a global entity");
        pinusClient.update(globalEntity);
        TestGlobalEntity loadEntity = new TestGlobalEntity();
        loadEntity.setId(globalEntity.getId());
        pinusClient.load(loadEntity, false);
        Assert.assertEquals(globalEntity, loadEntity);

        // save and update more
        globalEntities = new ArrayList<TestGlobalEntity>();
        for (int i = 0; i < 5; i++) {
            globalEntities.add(createGlobalEntity());
        }
        pinusClient.saveBatch(globalEntities);
        for (int i = 0; i < globalEntities.size(); i++) {
            globalEntities.get(i).setTestString("i am a global entity batch");
        }
        pinusClient.updateBatch(globalEntities);
        IQuery<TestGlobalEntity> query = pinusClient.createQuery(TestGlobalEntity.class);
        query.and(Condition.eq("testString", "i am a global entity batch"));
        List<TestGlobalEntity> queryList = query.list();
        for (int i = 0; i < queryList.size(); i++) {
            Assert.assertEquals(globalEntities.get(i), queryList.get(i));
        }

        // save and update one
        shardingEntity1 = createEntity();
        shardingEntity1.setTestInt(1);
        pinusClient.save(shardingEntity1);
        shardingEntity1.setTestString("i am a sharding entity");
        pinusClient.update(shardingEntity1);
        TestEntity testEntity = new TestEntity();
        testEntity.setId(shardingEntity1.getId());
        testEntity.setTestInt(shardingEntity1.getTestInt());
        testEntity.load();

        shardingEntity2 = createEntity();
        shardingEntity2.setTestInt(2);
        pinusClient.save(shardingEntity2);
        shardingEntity2.setTestString("i am a sharding entity");
        pinusClient.update(shardingEntity2);

        // save and update more
        shardingEntities = new ArrayList<TestEntity>(5);
        for (int i = 0; i < 5; i++) {
            shardingEntities.add(createEntity());
        }
        pinusClient.saveBatch(shardingEntities);
        for (int i = 0; i < shardingEntities.size(); i++) {
            shardingEntities.get(i).setTestString("i am a sharding entity batch");
        }
        pinusClient.updateBatch(shardingEntities);
    }

    /**
     * clean data.
     */
    @AfterClass
    public static void after() {
        // remove one
        pinusClient.delete(globalEntity);
        // check remove one
        try {
            pinusClient.load(globalEntity);
            Assert.assertTrue(false);
        } catch (DBOperationException e) {
            Assert.assertTrue(true);
        }

        // remove more
        pinusClient.delete(globalEntities);
        // check remove more
        IQuery<TestGlobalEntity> globalDeleteQ = pinusClient.createQuery(TestGlobalEntity.class);
        List<Long> globalPks = Lists.newArrayList();
        for (TestGlobalEntity entity : globalEntities) {
            globalPks.add(entity.getId());
        }
        globalDeleteQ.and(Condition.in("pk", globalPks));
        Assert.assertEquals(0, globalDeleteQ.count().intValue());

        // remove one
        pinusClient.delete(shardingEntity1);
        pinusClient.delete(shardingEntity2);
        // check remove one
        try {
            pinusClient.load(shardingEntity1);
            Assert.assertTrue(false);
        } catch (DBOperationException e) {
            Assert.assertTrue(true);
        }
        try {
            pinusClient.load(shardingEntity2);
            Assert.assertTrue(false);
        } catch (DBOperationException e) {
            Assert.assertTrue(true);
        }

        // remove more
        pinusClient.delete(shardingEntities);
        // check remove more
        IQuery<TestEntity> shardingDeleteQ = pinusClient.createQuery(TestEntity.class);
        List<Long> shardingPks = Lists.newArrayList();
        for (TestEntity entity : shardingEntities) {
            shardingPks.add(entity.getId());
        }
        shardingDeleteQ.and(Condition.in("id", shardingPks));
        Assert.assertEquals(0, shardingDeleteQ.count().intValue());

        pinusClient.destroy();
    }

    @Test
    public void testCount() {
        IQuery<TestGlobalEntity> globalQuery = pinusClient.createQuery(TestGlobalEntity.class);
        Assert.assertEquals(6, globalQuery.count().longValue());

        IQuery<TestEntity> shardingQuery = pinusClient.createQuery(TestEntity.class);
        Assert.assertEquals(7, shardingQuery.count().longValue());
    }

    @Test
    public void testSort() {
        IQuery<TestGlobalEntity> globalQuery = pinusClient.createQuery(TestGlobalEntity.class);
        globalQuery.orderBy("testLong", Order.DESC).orderBy("pk", Order.ASC);
        for (TestGlobalEntity entity : globalQuery.list()) {
            System.out.println(entity);
        }
    }

}
