package org.pinus4j.api;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.entity.TestGlobalEntity;

public class UpdateTimeTest extends BaseTest {

    private static IShardingStorageClient storageClient;

    @BeforeClass
    public static void beforeClass() {
        storageClient = getStorageClient();
    }

    @AfterClass
    public static void afterClass() {
        storageClient.destroy();
    }

    @Test
    public void testSave() {
        TestEntity entity = createEntity();
        TestGlobalEntity globalEntity = createGlobalEntity();

        try {
            storageClient.beginTransaction();
            storageClient.save(entity);
            storageClient.globalSave(globalEntity);
            storageClient.commit();
        } catch (Exception e) {
            storageClient.rollback();
        }
    }

    @Test
    public void testUpdate() {
        try {
            storageClient.beginTransaction();
            TestGlobalEntity globalEntity = storageClient.findByPk(34737, TestGlobalEntity.class);
            storageClient.globalUpdate(globalEntity);

//            TestEntity entity = storageClient.findByPk(32896, TestEntity.class);
//            storageClient.update(entity);
            storageClient.commit();
        } catch (Exception e) {
            storageClient.rollback();
        }
    }

}
