package org.pinus4j.api;

import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.impl.Condition;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.entity.TestGlobalEntity;
import org.pinus4j.entity.TestGlobalUnionKeyEntity;

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
        TestGlobalEntity globalEntity = createGlobalEntity();
        globalEntity.setId(pinusClient.genClusterUniqueLongId("test"));
        TestGlobalUnionKeyEntity globalUKEntity = createGlobalUnionKeyEntity();
        TestEntity shardingEntity = createEntity();
        shardingEntity.setId(pinusClient.genClusterUniqueLongId("test"));

//        pinusClient.save(globalEntity);
        System.out.println(globalEntity.getId());
        pinusClient.save(globalUKEntity);
        System.out.println(globalUKEntity);
//        pinusClient.save(shardingEntity);
        System.out.println(shardingEntity.getId());
    }

    @Test
    public void testUpdate() throws Exception {
        TestGlobalEntity globalEntity = new TestGlobalEntity();
        globalEntity.setId(1);
        pinusClient.load(globalEntity);
        System.out.println(globalEntity);

        globalEntity.setTestString("hello pinus");
        pinusClient.update(globalEntity);
    }

    @Test
    public void testSaveBatch() throws Exception {
        List<TestGlobalEntity> globalList = Lists.newArrayList(createGlobalEntity(), createGlobalEntity(),
                createGlobalEntity());

        List<TestGlobalUnionKeyEntity> globalUKList = Lists.newArrayList(createGlobalUnionKeyEntity(),
                createGlobalUnionKeyEntity(), createGlobalUnionKeyEntity());

        List<TestEntity> shardingList = Lists.newArrayList(createEntity(), createEntity(), createEntity());

        pinusClient.saveBatch(globalList, false);
        pinusClient.saveBatch(globalUKList, false);
        pinusClient.saveBatch(shardingList, false);
    }

    @Test
    public void testDelete() throws Exception {
        List<TestGlobalEntity> globalList = Lists.newArrayList();
        TestGlobalEntity globalEntity = createGlobalEntity();
        globalEntity.setId(4);
        globalList.add(globalEntity);
        pinusClient.delete(globalList);
        
        TestGlobalUnionKeyEntity globalUNEntity = createGlobalUnionKeyEntity();
        globalUNEntity.setId("fgcadcdibg");
        pinusClient.delete(globalUNEntity);
    }

    @Test
    public void testQuery() throws Exception {
        IQuery query = pinusClient.createQuery(TestGlobalEntity.class);
        query.and(Condition.eq("testInt", 5));
        List<TestGlobalEntity> list = query.list();
        for (TestGlobalEntity entity : list) {
            System.out.println(entity);
        }

        query.clean();
        query.and(Condition.in("pk", 1, 2, 3, 4));
        query.list();
    }

}
