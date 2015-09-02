package org.pinus4j.api;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.impl.Condition;
import org.pinus4j.api.query.impl.Order;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.entity.TestGlobalEntity;

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
    //    @BeforeClass
    //    public static void before() {
    //        // save and update global one
    //        globalEntity = createGlobalEntity();
    //        globalEntity.setoTestLong(8l);
    //        pinusClient.save(globalEntity);
    //        globalEntity.setTestString("i am a global entity");
    //        pinusClient.update(globalEntity);
    //        TestGlobalEntity loadEntity = new TestGlobalEntity();
    //        loadEntity.setId(globalEntity.getId());
    //        pinusClient.load(loadEntity, false);
    //
    //        // save and update more
    //        globalEntities = new ArrayList<TestGlobalEntity>();
    //        for (int i = 0; i < 5; i++) {
    //            globalEntities.add(createGlobalEntity());
    //        }
    //        pinusClient.saveBatch(globalEntities);
    //        for (int i = 0; i < globalEntities.size(); i++) {
    //            globalEntities.get(i).setTestString("i am a global entity batch");
    //        }
    //        pinusClient.updateBatch(globalEntities);
    //
    //        // save and update one
    //        shardingEntity1 = createEntity();
    //        shardingEntity1.setTestInt(1);
    //        pinusClient.save(shardingEntity1);
    //        shardingEntity1.setTestString("i am a sharding entity");
    //        pinusClient.update(shardingEntity1);
    //        TestEntity testEntity = new TestEntity();
    //        testEntity.setId(shardingEntity1.getId());
    //        testEntity.setTestInt(shardingEntity1.getTestInt());
    //        testEntity.load();
    //
    //        shardingEntity2 = createEntity();
    //        shardingEntity2.setTestInt(2);
    //        pinusClient.save(shardingEntity2);
    //        shardingEntity2.setTestString("i am a sharding entity");
    //        pinusClient.update(shardingEntity2);
    //
    //        // save and update more
    //        shardingEntities = new ArrayList<TestEntity>(5);
    //        for (int i = 0; i < 5; i++) {
    //            shardingEntities.add(createEntity());
    //        }
    //        pinusClient.saveBatch(shardingEntities);
    //        for (int i = 0; i < shardingEntities.size(); i++) {
    //            shardingEntities.get(i).setTestString("i am a sharding entity batch");
    //        }
    //        pinusClient.updateBatch(shardingEntities);
    //    }

    /**
     * clean data.
     */
    //    @AfterClass
    //    public static void after() {
    //        // remove one
    //        pinusClient.delete(globalEntity);
    //        // check remove one
    //        try {
    //            pinusClient.load(globalEntity);
    //            Assert.assertTrue(false);
    //        } catch (DBOperationException e) {
    //            Assert.assertTrue(true);
    //        }
    //
    //        // remove more
    //        pinusClient.delete(globalEntities);
    //        // check remove more
    //        IQuery<TestGlobalEntity> globalDeleteQ = pinusClient.createQuery(TestGlobalEntity.class);
    //        List<Long> globalPks = Lists.newArrayList();
    //        for (TestGlobalEntity entity : globalEntities) {
    //            globalPks.add(entity.getId());
    //        }
    //        globalDeleteQ.and(Condition.in("pk", globalPks));
    //        Assert.assertEquals(0, globalDeleteQ.count().intValue());
    //
    //        // remove one
    //        pinusClient.delete(shardingEntity1);
    //        pinusClient.delete(shardingEntity2);
    //        // check remove one
    //        try {
    //            pinusClient.load(shardingEntity1);
    //            Assert.assertTrue(false);
    //        } catch (DBOperationException e) {
    //            Assert.assertTrue(true);
    //        }
    //        try {
    //            pinusClient.load(shardingEntity2);
    //            Assert.assertTrue(false);
    //        } catch (DBOperationException e) {
    //            Assert.assertTrue(true);
    //        }
    //
    //        // remove more
    //        pinusClient.delete(shardingEntities);
    //        // check remove more
    //        IQuery<TestEntity> shardingDeleteQ = pinusClient.createQuery(TestEntity.class);
    //        List<Long> shardingPks = Lists.newArrayList();
    //        for (TestEntity entity : shardingEntities) {
    //            shardingPks.add(entity.getId());
    //        }
    //        shardingDeleteQ.and(Condition.in("id", shardingPks));
    //        Assert.assertEquals(0, shardingDeleteQ.count().intValue());
    //
    //        pinusClient.destroy();
    //    }

    @Test
    public void test() {
        IQuery<TestGlobalEntity> query = pinusClient.createQuery(TestGlobalEntity.class);
        query.orderBy("pk", Order.DESC);
        query.limit(5);
        for (TestGlobalEntity entity : query.list()) {
            System.out.println(entity);
        }
    }

    @Test
    public void testSave() {
        List<TestGlobalEntity> globalEntites = Lists.newArrayList();
        for (int i = 0; i < 100; i++) {
            globalEntites.add(createGlobalEntity());
        }
        pinusClient.saveBatch(globalEntites, false);
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
        globalQuery.limit(10);
        //        globalQuery.orderBy("pk", Order.DESC);
        for (TestGlobalEntity entity : globalQuery.list()) {
            System.out.println(entity);
        }
    }

    @Test
    public void testQuery() throws Exception {
        IQuery<TestGlobalEntity> query = pinusClient.createQuery(TestGlobalEntity.class);

        Long[] pks = new Long[] { 3l, 4l, 1l, 2l, 5l, 6l, 19l };
        query.and(Condition.in("pk", pks));
        //        query.orderBy("pk", Order.DESC);
        for (TestGlobalEntity data : query.list()) {
            System.out.println(data);
        }

        query.clean();

        int[] testInts = new int[] { 8871, 7350, 7566, 9714, 2910 };
        query.and(Condition.in("testInt", testInts));
        //        query.orderBy("pk", Order.DESC);
        for (TestGlobalEntity data : query.list()) {
            System.out.println(data);
        }

        query.clean();
        query.orderBy("pk", Order.DESC);
        query.limit(0, 10);
        for (TestGlobalEntity data : query.list()) {
            System.out.println(data);
        }
        query.limit(10, 10);
        for (TestGlobalEntity data : query.list()) {
            System.out.println(data);
        }
    }

    @Test
    public void testConcurrent() throws Exception {
        List<Thread> threads = Lists.newArrayList();
        for (int i = 0; i < 500; i++) {
            Thread t = new Thread(new Runnable() {

                @Override
                public void run() {
                    IQuery<TestGlobalEntity> globalQuery = pinusClient.createQuery(TestGlobalEntity.class);
                    for (int i = 0; i < 10000; i++) {
                        globalQuery.limit(100).list();
                    }
                }
            });
            t.start();
            threads.add(t);
            Thread.sleep(100);
        }

        for (Thread t : threads) {
            t.join();
        }
    }

    @Test
    public void genData() throws Exception {
        pinusClient.beginTransaction();
        TestGlobalEntity entity = null;
        for (int i = 0; i < 10000; i++) {
            entity = createGlobalEntity();
            pinusClient.save(entity);
        }
        pinusClient.commit();
    }
}
