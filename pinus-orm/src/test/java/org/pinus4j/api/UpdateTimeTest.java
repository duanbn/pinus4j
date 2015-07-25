package org.pinus4j.api;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.api.query.Condition;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.entity.TestGlobalEntity;
import org.pinus4j.entity.TestGlobalUnionKeyEntity;

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
        TestGlobalUnionKeyEntity globalUKEntity = createGlobalUnionKeyEntity();

        try {
            storageClient.beginTransaction();
            storageClient.save(entity);
            storageClient.globalSave(globalEntity);
            storageClient.globalSave(globalUKEntity);
            storageClient.commit();
        } catch (Exception e) {
            e.printStackTrace();
            storageClient.rollback();
        }
    }

    @Test
    public void testUpdate() {
        try {
            storageClient.beginTransaction();
            TestGlobalEntity globalEntity = storageClient.findByPk(14, TestGlobalEntity.class);
            storageClient.globalUpdate(globalEntity);

            IQuery query = storageClient.createQuery();
            query.add(Condition.eq("id", 1349897939));
            query.add(Condition.eq("testByte", (byte) -1));
            TestGlobalUnionKeyEntity globalUnionKeyEntity = storageClient.findOneByQuery(query,
                    TestGlobalUnionKeyEntity.class);
            storageClient.globalUpdate(globalUnionKeyEntity);

            //            TestEntity entity = storageClient.findByPk(32896, TestEntity.class);
            //            storageClient.update(entity);
            storageClient.commit();
        } catch (Exception e) {
            e.printStackTrace();
            storageClient.rollback();
        }
    }

}
