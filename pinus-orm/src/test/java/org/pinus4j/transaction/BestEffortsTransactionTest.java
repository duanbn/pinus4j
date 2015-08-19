package org.pinus4j.transaction;

import junit.framework.Assert;

import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.beans.ShardingKey;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.entity.TestGlobalEntity;

public class BestEffortsTransactionTest extends BaseTest {

    @Test
    public void testCommit() {
        TestGlobalEntity testGlobalEntity = createGlobalEntity();
        TestEntity testEntity = createEntity();

        pinusClient.beginTransaction();
        try {
            pinusClient.save(testGlobalEntity);
            pinusClient.save(testEntity);

            pinusClient.commit();

            TestGlobalEntity a = new TestGlobalEntity();
            a.setId(testGlobalEntity.getId());
            pinusClient.load(a);
            TestEntity b = new TestEntity();
            b.setId(testEntity.getId());
            b.setTestInt(testEntity.getTestInt());
            pinusClient.load(b);
            Assert.assertEquals(testGlobalEntity, a);
            Assert.assertEquals(testEntity, b);

            pinusClient.delete(testEntity);
            pinusClient.delete(testGlobalEntity);
        } catch (Exception e) {
            pinusClient.rollback();
        }
    }

    @Test
    public void testRollback() {
        long globalId = 1;
        long shardingId = 1;
        TestGlobalEntity testGlobalEntity = createGlobalEntity();
        testGlobalEntity.setId(globalId);
        TestEntity testEntity = createEntity();
        testEntity.setId(shardingId);

        pinusClient.beginTransaction();
        try {
            pinusClient.save(testGlobalEntity);
            pinusClient.save(testEntity);

            throw new RuntimeException();
        } catch (Exception e) {
            pinusClient.rollback();
        }
    }

}
