package org.pinus4j.cache;

import java.util.List;

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
import org.pinus4j.exceptions.DBClusterException;

import com.google.common.collect.Lists;

public class SecondCacheTest extends BaseTest {

    private static ISecondCache       secondCache;

    private static ShardingDBResource db;

    private String                    whereKey  = "where a='a' and b = 'b'";

    private String                    tableName = "testglobalentity";

    @BeforeClass
    public static void before() {
        secondCache = pinusClient.getDBCluster().getSecondCache();

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
    public void testGlobal() {
        List<TestGlobalEntity> datas = Lists.newArrayList();
        datas.add(createGlobalEntity());
        datas.add(createGlobalEntity());
        datas.add(createGlobalEntity());
        datas.add(createGlobalEntity());
        datas.add(createGlobalEntity());

        secondCache.putGlobal(whereKey, CLUSTER_KLSTORAGE, tableName, datas);

        List<TestGlobalEntity> datas1 = secondCache.getGlobal(whereKey, CLUSTER_KLSTORAGE, tableName);
        Assert.assertEquals(datas, datas1);

        secondCache.removeGlobal(CLUSTER_KLSTORAGE, tableName);
        List<TestGlobalEntity> datas2 = secondCache.getGlobal(whereKey, CLUSTER_KLSTORAGE, tableName);
        Assert.assertNull(datas2);
    }

    @Test
    public void testSharding() {
        List<TestEntity> datas = Lists.newArrayList();
        datas.add(createEntity());
        datas.add(createEntity());
        datas.add(createEntity());
        datas.add(createEntity());
        datas.add(createEntity());

        secondCache.put(whereKey, db, datas);

        List<TestEntity> datas1 = secondCache.get(whereKey, db);
        Assert.assertEquals(datas, datas1);

        secondCache.remove(db);
        List<TestEntity> datas2 = secondCache.get(whereKey, db);
        Assert.assertNull(datas2);
    }

}
