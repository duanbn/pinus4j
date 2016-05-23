package org.pinus4j.cache;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.beans.ShardingKey;
import org.pinus4j.cluster.resources.ShardingDBResource;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.entity.TestGlobalEntity;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.entity.meta.PKName;
import org.pinus4j.entity.meta.PKValue;
import org.pinus4j.exceptions.DBClusterException;

import com.google.common.collect.Maps;

public class PrimaryCacheTest extends BaseTest {

    private static String             tableName = "testglobalentity";

    private static ShardingDBResource db;

    private static IPrimaryCache      primaryCache;

    @BeforeClass
    public static void before() {
        primaryCache = pinusClient.getDBCluster().getPrimaryCache();

        IShardingKey<?> shardingValue = new ShardingKey<Integer>(CLUSTER_KLSTORAGE, 1);
        try {
            db = (ShardingDBResource) pinusClient.getDBCluster().selectDBResourceFromMaster("test_entity",
                    shardingValue);
        } catch (DBClusterException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void after() {
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
    }

    @Test
    public void testGlobal() {
        Map<EntityPK, TestGlobalEntity> data = Maps.newLinkedHashMap();

        for (int i = 1; i <= 5; i++) {
            TestGlobalEntity entity2 = createGlobalEntity();
            data.put(EntityPK.valueOf(new PKName[] { PKName.valueOf("pk") }, new PKValue[] { PKValue.valueOf(i) }),
                    entity2);
        }
        primaryCache.putGlobal(CLUSTER_KLSTORAGE, tableName, data);

        EntityPK[] entityPks = new EntityPK[] {
                EntityPK.valueOf(new PKName[] { PKName.valueOf("pk") }, new PKValue[] { PKValue.valueOf(1) }),
                EntityPK.valueOf(new PKName[] { PKName.valueOf("pk") }, new PKValue[] { PKValue.valueOf(2) }),
                EntityPK.valueOf(new PKName[] { PKName.valueOf("pk") }, new PKValue[] { PKValue.valueOf(3) }),
                EntityPK.valueOf(new PKName[] { PKName.valueOf("pk") }, new PKValue[] { PKValue.valueOf(4) }),
                EntityPK.valueOf(new PKName[] { PKName.valueOf("pk") }, new PKValue[] { PKValue.valueOf(5) }) };
        Map<EntityPK, TestGlobalEntity> entities1 = primaryCache.getGlobal(CLUSTER_KLSTORAGE, tableName, entityPks);
        for (EntityPK entityPk : entityPks) {
            Assert.assertEquals(data.get(entityPk), entities1.get(entityPk));
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
    }

    @Test
    public void testSharding() {
        // test more
        EntityPK[] entityPks = new EntityPK[] {
                EntityPK.valueOf(new PKName[] { PKName.valueOf("id") }, new PKValue[] { PKValue.valueOf(1) }),
                EntityPK.valueOf(new PKName[] { PKName.valueOf("id") }, new PKValue[] { PKValue.valueOf(2) }),
                EntityPK.valueOf(new PKName[] { PKName.valueOf("id") }, new PKValue[] { PKValue.valueOf(3) }),
                EntityPK.valueOf(new PKName[] { PKName.valueOf("id") }, new PKValue[] { PKValue.valueOf(4) }),
                EntityPK.valueOf(new PKName[] { PKName.valueOf("id") }, new PKValue[] { PKValue.valueOf(5) }) };
        Map<EntityPK, TestEntity> data = Maps.newHashMap();

        for (int i = 1; i <= 5; i++) {
            TestEntity entity2 = createEntity();
            data.put(EntityPK.valueOf(new PKName[] { PKName.valueOf("id") }, new PKValue[] { PKValue.valueOf(i) }),
                    entity2);
        }
        primaryCache.put(db, data);

        Map<EntityPK, TestEntity> entities1 = primaryCache.get(db, entityPks);
        for (EntityPK entityPk : entityPks) {
            Assert.assertEquals(data.get(entityPk), entities1.get(entityPk));
        }
        primaryCache.remove(db, Arrays.asList(entityPks));
        entities1 = primaryCache.get(db, entityPks);
        Assert.assertEquals(0, entities1.size());
    }
}
