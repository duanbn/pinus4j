package org.pinus4j.cache;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.beans.ShardingKey;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.entity.TestGlobalEntity;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.entity.meta.PKValue;
import org.pinus4j.exceptions.DBClusterException;

public class RedisPrimaryCacheImplTest extends BaseTest {

    private static String                 tableName = "test_entity";

    private static ShardingDBResource     db;

    private static IPrimaryCache          primaryCache;

    @BeforeClass
    public static void beforeClass() {
        primaryCache = pinusClient.getDBCluster().getPrimaryCache();

        IShardingKey<?> shardingValue = new ShardingKey<Integer>(CLUSTER_KLSTORAGE, 1);
        try {
            db = (ShardingDBResource) pinusClient.getDBCluster().selectDBResourceFromMaster(tableName, shardingValue);
        } catch (DBClusterException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        pinusClient.destroy();
    }

    @Test
    public void testGetAvailableServsers() {
        Collection<SocketAddress> servers = primaryCache.getAvailableServers();
        Assert.assertEquals(1, servers.size());
    }

    @Test
    public void testGlobalCount() {
        primaryCache.setCountGlobal(CLUSTER_KLSTORAGE, tableName, 10);
        long count = primaryCache.getCountGlobal(CLUSTER_KLSTORAGE, tableName);
        Assert.assertEquals(10, count);

        count = primaryCache.incrCountGlobal(CLUSTER_KLSTORAGE, tableName, 1);
        Assert.assertEquals(11, count);

        count = primaryCache.decrCountGlobal(CLUSTER_KLSTORAGE, tableName, 2);
        Assert.assertEquals(9, count);

        primaryCache.removeCountGlobal(CLUSTER_KLSTORAGE, tableName);
        count = primaryCache.getCountGlobal(CLUSTER_KLSTORAGE, tableName);
        Assert.assertEquals(-1, count);
    }

    @Test
    public void testGlobal() {
        TestGlobalEntity entity = createGlobalEntity();
        EntityPK entityPk = EntityPK.valueOf(null, new PKValue[] { PKValue.valueOf(100) });
        primaryCache.putGlobal(CLUSTER_KLSTORAGE, tableName, entityPk, entity);

        TestGlobalEntity entity1 = primaryCache.getGlobal(CLUSTER_KLSTORAGE, tableName, entityPk);
        Assert.assertEquals(entity, entity1);

        primaryCache.removeGlobal(CLUSTER_KLSTORAGE, tableName, entityPk);
        entity = primaryCache.getGlobal(CLUSTER_KLSTORAGE, tableName, entityPk);
        Assert.assertNull(entity);

        List<TestGlobalEntity> entities = new ArrayList<TestGlobalEntity>();
        for (int i = 1; i <= 5; i++) {
            TestGlobalEntity entity2 = createGlobalEntity();
            entity2.setId(i);
            entities.add(entity2);
        }
        primaryCache.putGlobal(CLUSTER_KLSTORAGE, tableName, entities);

        EntityPK[] entityPks = new EntityPK[] { EntityPK.valueOf(null, new PKValue[] { PKValue.valueOf(1) }),
                EntityPK.valueOf(null, new PKValue[] { PKValue.valueOf(2) }),
                EntityPK.valueOf(null, new PKValue[] { PKValue.valueOf(3) }),
                EntityPK.valueOf(null, new PKValue[] { PKValue.valueOf(4) }),
                EntityPK.valueOf(null, new PKValue[] { PKValue.valueOf(5) }) };
        List<TestGlobalEntity> entities1 = primaryCache.getGlobal(CLUSTER_KLSTORAGE, tableName, entityPks);
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(entities.get(i), entities1.get(i));
        }

        primaryCache.removeGlobal(CLUSTER_KLSTORAGE, tableName, Arrays.asList(entityPks));
        entities1 = primaryCache.getGlobal(CLUSTER_KLSTORAGE, tableName, entityPks);
        Assert.assertEquals(0, entities1.size());
    }

    @Test
    public void testShardingCount() {
        primaryCache.setCount(db, 10);
        long count = primaryCache.getCount(db);
        Assert.assertEquals(10, count);

        count = primaryCache.incrCount(db, 1);
        Assert.assertEquals(11, count);
        count = primaryCache.decrCount(db, 2);
        Assert.assertEquals(9, count);
        primaryCache.removeCount(db);
        count = primaryCache.getCount(db);
        Assert.assertEquals(-1, count);
    }

    @Test
    public void testSharding() {
        // test one
        TestEntity entity = createEntity();
        EntityPK entityPk = EntityPK.valueOf(null, new PKValue[] { PKValue.valueOf(100) });
        primaryCache.put(db, entityPk, entity);
        TestEntity entity1 = primaryCache.get(db, entityPk);
        Assert.assertEquals(entity, entity1);
        primaryCache.remove(db, entityPk);
        entity = primaryCache.get(db, entityPk);
        Assert.assertNull(entity);

        // test more
        EntityPK[] entityPks = new EntityPK[] { EntityPK.valueOf(null, new PKValue[] { PKValue.valueOf(1) }),
                EntityPK.valueOf(null, new PKValue[] { PKValue.valueOf(2) }),
                EntityPK.valueOf(null, new PKValue[] { PKValue.valueOf(3) }),
                EntityPK.valueOf(null, new PKValue[] { PKValue.valueOf(4) }),
                EntityPK.valueOf(null, new PKValue[] { PKValue.valueOf(5) }) };
        List<TestEntity> entities = new ArrayList<TestEntity>();
        for (int i = 1; i <= 5; i++) {
            TestEntity entity2 = createEntity();
            entity2.setId(i);
            entities.add(entity2);
        }
        primaryCache.put(db, entityPks, entities);

        List<TestEntity> entities1 = primaryCache.get(db, entityPks);
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(entities.get(i), entities1.get(i));
        }

        primaryCache.remove(db, Arrays.asList(entityPks));
        entities1 = primaryCache.get(db, entityPks);
        Assert.assertEquals(0, entities1.size());
    }

}
