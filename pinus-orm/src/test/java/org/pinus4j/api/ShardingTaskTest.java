package org.pinus4j.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.api.query.Condition;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.beans.ShardingKey;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.task.ITask;
import org.pinus4j.task.TaskFuture;

public class ShardingTaskTest extends BaseTest {

    private static Number[]               pks;

    private static IShardingKey<Integer>  moreKey = new ShardingKey<Integer>(CLUSTER_KLSTORAGE, 4);

    private static List<TestEntity>       entities;

    private static final int              SIZE    = 1100;

    private static IShardingStorageClient storageClient;

    @BeforeClass
    public static void before() {
        storageClient = getStorageClient();

        // save more
        entities = new ArrayList<TestEntity>(SIZE);
        TestEntity entity = null;
        for (int i = 0; i < SIZE; i++) {
            entity = createEntity();
            entity.setTestString("i am pinus");
            entities.add(entity);
        }
        pks = storageClient.saveBatch(entities, moreKey);
        // check save more
        entities = storageClient.findByPkList(Arrays.asList(pks), moreKey, TestEntity.class);
        Assert.assertEquals(SIZE, entities.size());
    }

    @AfterClass
    public static void after() {
        // remove more
        storageClient.removeByPks(moreKey, TestEntity.class, pks);

        storageClient.destroy();
    }

    @Test
    public void testSubmit() throws InterruptedException {
        ITask<TestEntity> task = new SimpleShardingTask();

        TaskFuture future = storageClient.submit(task, TestEntity.class);
        while (!future.isDone()) {
            System.out.println(future.getProgress());

            Thread.sleep(2000);
        }

        System.out.println(future);
    }

    @Test
    public void testSubmitQuery() throws InterruptedException {
        ITask<TestEntity> task = new SimpleShardingTask();
        IQuery query = storageClient.createQuery();
        query.add(Condition.gt("testInt", 100));

        TaskFuture future = storageClient.submit(task, TestEntity.class, query);
        future.await();

        System.out.println(future);
    }

    public static class SimpleShardingTask extends AbstractTask<TestEntity> {
        @Override
        public void batchRecord(List<TestEntity> entity) {
        }
    }

}
