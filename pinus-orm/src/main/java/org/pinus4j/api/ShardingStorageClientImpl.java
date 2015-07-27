/**
 * Copyright 2014 Duan Bingnan
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pinus4j.api;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import javax.transaction.TransactionManager;

import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.impl.DefaultQueryImpl;
import org.pinus4j.cluster.IDBCluster;
import org.pinus4j.cluster.IDBClusterBuilder;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.beans.ShardingKey;
import org.pinus4j.cluster.enums.EnumDB;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.cluster.enums.EnumSyncAction;
import org.pinus4j.cluster.impl.DefaultDBCluster;
import org.pinus4j.constant.Const;
import org.pinus4j.datalayer.IDataLayerBuilder;
import org.pinus4j.datalayer.JdbcDataLayerBuilder;
import org.pinus4j.datalayer.query.IGlobalQuery;
import org.pinus4j.datalayer.query.IShardingQuery;
import org.pinus4j.datalayer.update.IGlobalUpdate;
import org.pinus4j.datalayer.update.IShardingUpdate;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.entity.meta.PKName;
import org.pinus4j.entity.meta.PKValue;
import org.pinus4j.exceptions.DBClusterException;
import org.pinus4j.exceptions.DBOperationException;
import org.pinus4j.generator.IIdGenerator;
import org.pinus4j.task.ITask;
import org.pinus4j.task.TaskExecutor;
import org.pinus4j.task.TaskFuture;
import org.pinus4j.transaction.enums.EnumTransactionIsolationLevel;
import org.pinus4j.transaction.impl.BestEffortsOnePCJtaTransactionManager;
import org.pinus4j.utils.PKUtil;
import org.pinus4j.utils.CheckUtil;
import org.pinus4j.utils.ReflectUtil;
import org.pinus4j.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * sharding storage client implements. replace by DefaultPinusClient
 * 
 * @author duanbn
 */
@Deprecated
public class ShardingStorageClientImpl implements IShardingStorageClient {

    /**
     * 日志.
     */
    public static final Logger           LOG        = LoggerFactory.getLogger(ShardingStorageClientImpl.class);

    /**
     * reference it self;
     */
    public static IShardingStorageClient instance;

    /**
     * 数据库类型.
     */
    private EnumDB                       enumDb     = EnumDB.MYSQL;

    /**
     * 同步数据表操作.
     */
    private EnumSyncAction               syncAction = EnumSyncAction.CREATE;

    /**
     * 扫描数据对象的包. 数据对象是使用了@Table注解的javabean.
     */
    private String                       scanPackage;

    /**
     * id generator.
     */
    private IIdGenerator                 idGenerator;

    /**
     * 数据库集群引用.
     */
    private IDBCluster                   dbCluster;

    private TransactionManager           txManager;

    /**
     * global updater.
     */
    private IGlobalUpdate                globalUpdater;

    /**
     * global query.
     */
    private IGlobalQuery                 globalQuery;

    /**
     * 分库分表更新实现.
     */
    private IShardingUpdate              shardingUpdater;

    /**
     * sharding query.
     */
    private IShardingQuery               shardingQuery;

    /**
     * 初始化方法
     */
    public void init() {
        IDBClusterBuilder dbClusterBuilder = new DefaultDBCluster();
        dbClusterBuilder.setScanPackage(this.scanPackage);
        dbClusterBuilder.setSyncAction(this.syncAction);
        dbClusterBuilder.setDbType(this.enumDb);
        this.dbCluster = dbClusterBuilder.build();

        this.idGenerator = this.dbCluster.getIdGenerator();

        this.txManager = this.dbCluster.getTransactionManager();

        //
        // 初始化分库分表增删改查实现.
        //
        IDataLayerBuilder dataLayerBuilder = JdbcDataLayerBuilder.valueOf(dbCluster);
        dataLayerBuilder.setPrimaryCache(this.dbCluster.getPrimaryCache());
        dataLayerBuilder.setSecondCache(this.dbCluster.getSecondCache());

        this.globalUpdater = dataLayerBuilder.buildGlobalUpdate(this.dbCluster.getIdGenerator());
        this.globalQuery = dataLayerBuilder.buildGlobalQuery();

        this.shardingUpdater = dataLayerBuilder.buildShardingUpdate(this.dbCluster.getIdGenerator());
        this.shardingQuery = dataLayerBuilder.buildShardingQuery();

        // FashionEntity dependency this.
        instance = this;
    }

    // ////////////////////////////////////////////////////////
    // 事务相关
    // ////////////////////////////////////////////////////////
    @Override
    public void beginTransaction() {
        beginTransaction(EnumTransactionIsolationLevel.READ_COMMITTED);
    }

    @Override
    public void beginTransaction(EnumTransactionIsolationLevel txLevel) {
        ((BestEffortsOnePCJtaTransactionManager) this.txManager).setTransactionIsolationLevel(txLevel);
        try {
            this.txManager.begin();
        } catch (Exception e) {
            throw new DBOperationException(e);
        }
    }

    @Override
    public void commit() {
        try {
            this.txManager.commit();
        } catch (Exception e) {
            throw new DBOperationException(e);
        }
    }

    @Override
    public void rollback() {
        try {
            this.txManager.rollback();
        } catch (Exception e) {
            throw new DBOperationException(e);
        }
    }

    // ////////////////////////////////////////////////////////
    // 数据处理相关
    // ////////////////////////////////////////////////////////
    @Override
    public <T> TaskFuture submit(ITask<T> task, Class<T> clazz) {
        TaskExecutor<T> taskExecutor = new TaskExecutor<T>(clazz, this.dbCluster);
        return taskExecutor.execute(task);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public <T> TaskFuture submit(ITask<T> task, Class<T> clazz, IQuery query) {
        TaskExecutor taskExecutor = new TaskExecutor(clazz, this.dbCluster);
        return taskExecutor.execute(task, query);
    }

    // ////////////////////////////////////////////////////////
    // save相关
    // ////////////////////////////////////////////////////////
    @Override
    public Number globalSave(Object entity) {
        CheckUtil.checkGlobalEntity(entity);

        String clusterName = ReflectUtil.getClusterName(entity.getClass());
        CheckUtil.checkClusterName(clusterName);

        PKValue pkValue = this.globalUpdater.save(entity, clusterName);

        if (pkValue != null) {
            return pkValue.getValueAsNumber();
        }

        return null;
    }

    @Override
    public Number[] globalSaveBatch(List<? extends Object> entities, String clusterName) {
        CheckUtil.checkEntityList(entities);
        CheckUtil.checkClusterName(clusterName);

        PKValue[] pkValues = this.globalUpdater.saveBatch(entities, clusterName);
        Number[] pkNumbers = new Number[pkValues.length];
        for (int i = 0; i < pkValues.length; i++) {
            pkNumbers[i] = pkValues[i].getValueAsNumber();
        }

        return pkNumbers;
    }

    @Override
    public void globalUpdate(Object entity) {
        CheckUtil.checkGlobalEntity(entity);

        String clusterName = ReflectUtil.getClusterName(entity.getClass());
        CheckUtil.checkClusterName(clusterName);

        this.globalUpdater.update(entity, clusterName);
    }

    @Override
    public void globalUpdateBatch(List<? extends Object> entities, String clusterName) {
        CheckUtil.checkEntityList(entities);
        CheckUtil.checkClusterName(clusterName);

        this.globalUpdater.updateBatch(entities, clusterName);
    }

    @Override
    public void globalRemoveByPk(Number pk, Class<?> clazz, String clusterName) {
        CheckUtil.checkNumberGtZero(pk);
        CheckUtil.checkClass(clazz);
        CheckUtil.checkClusterName(clusterName);

        PKName[] pkNames = new PKName[] { ReflectUtil.getNotUnionPkName(clazz) };
        PKValue[] pkValues = new PKValue[] { PKValue.valueOf(pk) };

        this.globalUpdater.removeByPk(EntityPK.valueOf(pkNames, pkValues), clazz, clusterName);
    }

    @Override
    public void globalRemoveByPkList(List<? extends Number> pks, Class<?> clazz, String clusterName) {
        if (pks == null || pks.isEmpty()) {
            return;
        }
        CheckUtil.checkClass(clazz);
        CheckUtil.checkClusterName(clusterName);

        List<EntityPK> entityPkList = Lists.newArrayListWithCapacity(pks.size());
        PKName[] pkNames = new PKName[] { ReflectUtil.getNotUnionPkName(clazz) };
        PKValue[] pkValues = null;
        for (Number pk : pks) {
            pkValues = new PKValue[] { PKValue.valueOf(pk) };
            entityPkList.add(EntityPK.valueOf(pkNames, pkValues));
        }

        this.globalUpdater.removeByPks(entityPkList, clazz, clusterName);
    }

    @Override
    public void globalRemoveByPks(String clusterName, Class<?> clazz, Number... pks) {
        if (pks == null || pks.length == 0) {
            return;
        }

        globalRemoveByPkList(Arrays.asList(pks), clazz, clusterName);
    }

    @Override
    public Number save(Object entity) {
        CheckUtil.checkShardingEntity(entity);

        String clusterName = ReflectUtil.getClusterName(entity.getClass());
        Object shardingKey = ReflectUtil.getShardingValue(entity);
        IShardingKey<Object> sk = new ShardingKey<Object>(clusterName, shardingKey);
        CheckUtil.checkShardingKey(sk);

        return this.shardingUpdater.save(entity, sk).getValueAsNumber();
    }

    @Override
    public void update(Object entity) {
        CheckUtil.checkShardingEntity(entity);

        String clusterName = ReflectUtil.getClusterName(entity.getClass());
        Object shardingKey = ReflectUtil.getShardingValue(entity);
        IShardingKey<Object> sk = new ShardingKey<Object>(clusterName, shardingKey);
        CheckUtil.checkShardingKey(sk);

        this.shardingUpdater.update(entity, sk);
    }

    @Override
    public Number[] saveBatch(List<? extends Object> entities, IShardingKey<?> shardingKey) {
        CheckUtil.checkEntityList(entities);
        CheckUtil.checkShardingKey(shardingKey);

        PKValue[] pkValues = this.shardingUpdater.saveBatch(entities, shardingKey);

        return PKUtil.parseNumberArray(pkValues);
    }

    @Override
    public void updateBatch(List<? extends Object> entities, IShardingKey<?> shardingKey) {
        CheckUtil.checkEntityList(entities);
        CheckUtil.checkShardingKey(shardingKey);

        this.shardingUpdater.updateBatch(entities, shardingKey);
    }

    @Override
    public void removeByPk(Number pk, IShardingKey<?> shardingKey, Class<?> clazz) {
        CheckUtil.checkNumberGtZero(pk);
        CheckUtil.checkShardingKey(shardingKey);
        CheckUtil.checkClass(clazz);

        PKName[] pkNames = new PKName[] { ReflectUtil.getNotUnionPkName(clazz) };
        PKValue[] pkValues = new PKValue[] { PKValue.valueOf(pk) };
        this.shardingUpdater.removeByPk(EntityPK.valueOf(pkNames, pkValues), shardingKey, clazz);
    }

    @Override
    public void removeByPkList(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<?> clazz) {
        if (pks == null || pks.isEmpty()) {
            return;
        }
        CheckUtil.checkShardingKey(shardingKey);
        CheckUtil.checkClass(clazz);

        List<EntityPK> entityPkList = Lists.newArrayListWithCapacity(pks.size());
        PKName[] pkNames = new PKName[] { ReflectUtil.getNotUnionPkName(clazz) };
        PKValue[] pkValues = null;
        for (Number pk : pks) {
            pkValues = new PKValue[] { PKValue.valueOf(pk) };
            entityPkList.add(EntityPK.valueOf(pkNames, pkValues));
        }

        this.shardingUpdater.removeByPks(entityPkList, shardingKey, clazz);
    }

    @Override
    public void removeByPks(IShardingKey<?> shardingKey, Class<?> clazz, Number... pks) {
        if (pks == null || pks.length == 0) {
            return;
        }

        removeByPkList(Arrays.asList(pks), shardingKey, clazz);
    }

    // ////////////////////////////////////////////////////////
    // query相关
    // ////////////////////////////////////////////////////////

    @Override
    public Number getCount(Class<?> clazz) {
        return getCount(clazz, true);
    }

    @Override
    public Number getCount(Class<?> clazz, boolean useCache) {
        return getCount(clazz, useCache, EnumDBMasterSlave.MASTER);
    }

    @Override
    public Number getCount(Class<?> clazz, EnumDBMasterSlave masterSlave) {
        return getCount(clazz, true, masterSlave);
    }

    @Override
    public Number getCount(Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
        CheckUtil.checkClass(clazz);

        if (ReflectUtil.isShardingEntity(clazz)) {
            return this.shardingQuery.getCount(clazz, useCache, masterSlave);
        } else {
            return this.globalQuery.getCount(clazz, useCache, masterSlave);
        }
    }

    @Override
    public Number getCount(IShardingKey<?> shardingKey, Class<?> clazz) {
        return getCount(shardingKey, clazz, true);
    }

    @Override
    public Number getCount(IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache) {
        return getCount(shardingKey, clazz, useCache, EnumDBMasterSlave.MASTER);
    }

    @Override
    public Number getCount(IShardingKey<?> shardingKey, Class<?> clazz, EnumDBMasterSlave master) {
        return getCount(shardingKey, clazz, true, master);
    }

    @Override
    public Number getCount(IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
        CheckUtil.checkShardingKey(shardingKey);
        CheckUtil.checkClass(clazz);

        return this.shardingQuery.getCount(shardingKey, clazz, useCache, masterSlave);
    }

    @Override
    public Number getCountByQuery(Class<?> clazz, IQuery query) {
        return getCountByQuery(clazz, query, true);
    }

    @Override
    public Number getCountByQuery(Class<?> clazz, IQuery query, boolean useCache) {
        return getCountByQuery(clazz, query, useCache, EnumDBMasterSlave.MASTER);
    }

    @Override
    public Number getCountByQuery(Class<?> clazz, IQuery query, EnumDBMasterSlave masterSlave) {
        return getCountByQuery(clazz, query, true, masterSlave);
    }

    @Override
    public Number getCountByQuery(Class<?> clazz, IQuery query, boolean useCache, EnumDBMasterSlave masterSlave) {
        CheckUtil.checkClass(clazz);

        if (ReflectUtil.isShardingEntity(clazz)) {
            return this.shardingQuery.getCountByQuery(query, clazz, useCache, masterSlave);
        } else {
            return this.globalQuery.getCountByQuery(query, clazz, useCache, masterSlave);
        }
    }

    @Override
    public Number getCountByQuery(IQuery query, IShardingKey<?> shardingKey, Class<?> clazz) {
        return getCountByQuery(query, shardingKey, clazz, true);
    }

    @Override
    public Number getCountByQuery(IQuery query, IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache) {
        return getCountByQuery(query, shardingKey, clazz, useCache, EnumDBMasterSlave.MASTER);
    }

    @Override
    public Number getCountByQuery(IQuery query, IShardingKey<?> shardingKey, Class<?> clazz,
                                  EnumDBMasterSlave masterSlave) {
        return getCountByQuery(query, shardingKey, clazz, true, masterSlave);
    }

    @Override
    public Number getCountByQuery(IQuery query, IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache,
                                  EnumDBMasterSlave masterSlave) {
        CheckUtil.checkClass(clazz);
        CheckUtil.checkShardingKey(shardingKey);

        return this.shardingQuery.getCountByQuery(query, shardingKey, clazz, useCache, masterSlave);
    }

    @Override
    public <T> T findByPk(Number pk, Class<T> clazz) {
        return findByPk(pk, clazz, true);
    }

    @Override
    public <T> T findByPk(Number pk, Class<T> clazz, boolean useCache) {
        return findByPk(pk, clazz, useCache, EnumDBMasterSlave.MASTER);
    }

    @Override
    public <T> T findByPk(Number pk, Class<T> clazz, EnumDBMasterSlave masterSlave) {
        return findByPk(pk, clazz, true, masterSlave);
    }

    @Override
    public <T> T findByPk(Number pk, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
        CheckUtil.checkClass(clazz);

        PKName[] pkNames = new PKName[] { ReflectUtil.getNotUnionPkName(clazz) };
        PKValue[] pkValues = new PKValue[] { PKValue.valueOf(pk) };

        if (ReflectUtil.isShardingEntity(clazz)) {
            return this.shardingQuery.findByPk(EntityPK.valueOf(pkNames, pkValues), clazz, useCache, masterSlave);
        } else {
            return this.globalQuery.findByPk(EntityPK.valueOf(pkNames, pkValues), clazz, useCache, masterSlave);
        }
    }

    @Override
    public <T> List<T> findByPkList(List<? extends Number> pkList, Class<T> clazz) {
        return findByPkList(pkList, clazz, true);
    }

    @Override
    public <T> List<T> findByPkList(List<? extends Number> pkList, Class<T> clazz, boolean useCache) {
        return findByPkList(pkList, clazz, useCache, EnumDBMasterSlave.MASTER);
    }

    @Override
    public <T> List<T> findByPkList(List<? extends Number> pkList, Class<T> clazz, EnumDBMasterSlave masterSlave) {
        return findByPkList(pkList, clazz, true, masterSlave);
    }

    @Override
    public <T> List<T> findByPkList(List<? extends Number> pkList, Class<T> clazz, boolean useCache,
                                    EnumDBMasterSlave masterSlave) {
        CheckUtil.checkClass(clazz);

        List<EntityPK> entityPkList = Lists.newArrayList();
        PKName[] pkNames = new PKName[] { ReflectUtil.getNotUnionPkName(clazz) };
        for (Number pkValue : pkList) {
            PKValue[] pkValues = new PKValue[] { PKValue.valueOf(pkValue) };
            entityPkList.add(EntityPK.valueOf(pkNames, pkValues));
        }

        if (ReflectUtil.isShardingEntity(clazz)) {
            return this.shardingQuery.findByPkList(entityPkList, clazz, useCache, masterSlave);
        } else {
            return this.globalQuery.findByPkList(entityPkList, clazz, useCache, masterSlave);
        }
    }

    @Override
    public <T> T findOneByQuery(IQuery query, Class<T> clazz) {
        return findOneByQuery(query, clazz, true);
    }

    @Override
    public <T> T findOneByQuery(IQuery query, Class<T> clazz, boolean useCache) {
        return findOneByQuery(query, clazz, useCache, EnumDBMasterSlave.MASTER);
    }

    @Override
    public <T> T findOneByQuery(IQuery query, Class<T> clazz, EnumDBMasterSlave masterSlave) {
        return findOneByQuery(query, clazz, true, masterSlave);
    }

    @Override
    public <T> T findOneByQuery(IQuery query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
        CheckUtil.checkClass(clazz);

        if (ReflectUtil.isShardingEntity(clazz)) {
            return this.shardingQuery.findOneByQuery(query, clazz, useCache, masterSlave);
        } else {
            return this.globalQuery.findOneByQuery(query, clazz, useCache, masterSlave);
        }
    }

    @Override
    public <T> List<T> findByQuery(IQuery query, Class<T> clazz) {
        return findByQuery(query, clazz, true);
    }

    @Override
    public <T> List<T> findByQuery(IQuery query, Class<T> clazz, boolean useCache) {
        return findByQuery(query, clazz, useCache, EnumDBMasterSlave.MASTER);
    }

    @Override
    public <T> List<T> findByQuery(IQuery query, Class<T> clazz, EnumDBMasterSlave masterSlave) {
        return findByQuery(query, clazz, true, masterSlave);
    }

    @Override
    public <T> List<T> findByQuery(IQuery query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave) {
        CheckUtil.checkClass(clazz);

        if (ReflectUtil.isShardingEntity(clazz)) {
            return this.shardingQuery.findByQuery(query, clazz, useCache, masterSlave);
        } else {
            return this.globalQuery.findByQuery(query, clazz, useCache, masterSlave);
        }
    }

    @Override
    public List<Map<String, Object>> findBySql(SQL sql, Class<?> clazz) {
        return findBySql(sql, clazz, EnumDBMasterSlave.MASTER);
    }

    @Override
    public List<Map<String, Object>> findBySql(SQL sql, Class<?> clazz, EnumDBMasterSlave masterSlave) {
        CheckUtil.checkSQL(sql);
        CheckUtil.checkClass(clazz);

        if (ReflectUtil.isShardingEntity(clazz)) {
            return this.shardingQuery.findBySql(sql, masterSlave);
        } else {
            return this.globalQuery.findBySql(sql, clazz, masterSlave);
        }
    }

    @Override
    public <T> T findByPk(Number pk, IShardingKey<?> shardingKey, Class<T> clazz) {
        return findByPk(pk, shardingKey, clazz, true);
    }

    @Override
    public <T> T findByPk(Number pk, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache) {
        return findByPk(pk, shardingKey, clazz, useCache, EnumDBMasterSlave.MASTER);
    }

    @Override
    public <T> T findByPk(Number pk, IShardingKey<?> shardingKey, Class<T> clazz, EnumDBMasterSlave masterSlave) {
        return findByPk(pk, shardingKey, clazz, true, masterSlave);
    }

    @Override
    public <T> T findByPk(Number pk, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
                          EnumDBMasterSlave masterSlave) {
        CheckUtil.checkNumberGtZero(pk);
        CheckUtil.checkShardingKey(shardingKey);
        CheckUtil.checkClass(clazz);

        PKName[] pkNames = new PKName[] { ReflectUtil.getNotUnionPkName(clazz) };
        PKValue[] pkValues = new PKValue[] { PKValue.valueOf(pk) };

        return this.shardingQuery.findByPk(EntityPK.valueOf(pkNames, pkValues), shardingKey, clazz, useCache,
                masterSlave);
    }

    @Override
    public <T> List<T> findByPkList(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<T> clazz) {
        return findByPkList(pks, shardingKey, clazz, true);
    }

    @Override
    public <T> List<T> findByPkList(List<? extends Number> pkList, IShardingKey<?> shardingKey, Class<T> clazz,
                                    boolean useCache) {
        return findByPkList(pkList, shardingKey, clazz, useCache, EnumDBMasterSlave.MASTER);
    }

    @Override
    public <T> List<T> findByPkList(List<? extends Number> pks, IShardingKey<?> shardingKey, Class<T> clazz,
                                    EnumDBMasterSlave masterSlave) {
        return findByPkList(pks, shardingKey, clazz, true, masterSlave);
    }

    @Override
    public <T> List<T> findByPkList(List<? extends Number> pkList, IShardingKey<?> shardingKey, Class<T> clazz,
                                    boolean useCache, EnumDBMasterSlave masterSlave) {
        CheckUtil.checkNumberList(pkList);
        CheckUtil.checkShardingKey(shardingKey);
        CheckUtil.checkClass(clazz);

        List<EntityPK> entityPkList = Lists.newArrayList();
        PKName[] pkNames = new PKName[] { ReflectUtil.getNotUnionPkName(clazz) };
        for (Number pkValue : pkList) {
            PKValue[] pkValues = new PKValue[] { PKValue.valueOf(pkValue) };
            entityPkList.add(EntityPK.valueOf(pkNames, pkValues));
        }

        return this.shardingQuery.findByPkList(entityPkList, shardingKey, clazz, useCache, masterSlave);
    }

    @Override
    public <T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz) {
        return findOneByQuery(query, shardingKey, clazz, true);
    }

    @Override
    public <T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache) {
        return findOneByQuery(query, shardingKey, clazz, useCache, EnumDBMasterSlave.MASTER);
    }

    @Override
    public <T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, EnumDBMasterSlave masterSlave) {
        return findOneByQuery(query, shardingKey, clazz, true, masterSlave);
    }

    @Override
    public <T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
                                EnumDBMasterSlave masterSlave) {
        CheckUtil.checkShardingKey(shardingKey);
        CheckUtil.checkClass(clazz);

        return this.shardingQuery.findOneByQuery(query, shardingKey, clazz, useCache, masterSlave);
    }

    @Override
    public <T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz) {
        return findByQuery(query, shardingKey, clazz, true);
    }

    @Override
    public <T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache) {
        return findByQuery(query, shardingKey, clazz, useCache, EnumDBMasterSlave.MASTER);
    }

    @Override
    public <T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz,
                                   EnumDBMasterSlave masterSlave) {
        return findByQuery(query, shardingKey, clazz, true, masterSlave);
    }

    @Override
    public <T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
                                   EnumDBMasterSlave masterSlave) {
        CheckUtil.checkQuery(query);
        CheckUtil.checkShardingKey(shardingKey);
        CheckUtil.checkClass(clazz);

        return this.shardingQuery.findByQuery(query, shardingKey, clazz, useCache, masterSlave);
    }

    @Override
    public List<Map<String, Object>> findBySql(SQL sql, IShardingKey<?> shardingKey) {
        return findBySql(sql, shardingKey, EnumDBMasterSlave.MASTER);
    }

    @Override
    public List<Map<String, Object>> findBySql(SQL sql, IShardingKey<?> shardingKey, EnumDBMasterSlave masterSlave) {
        CheckUtil.checkShardingKey(shardingKey);
        CheckUtil.checkSQL(sql);

        return this.shardingQuery.findBySql(sql, shardingKey, masterSlave);
    }

    @Override
    public IDBCluster getDBCluster() {
        return this.dbCluster;
    }

    @Override
    public int genClusterUniqueIntId(String name) {
        return this.idGenerator.genClusterUniqueIntId(Const.ZK_SEQUENCE, name);
    }

    @Override
    public long genClusterUniqueLongId(String name) {
        return this.idGenerator.genClusterUniqueLongId(Const.ZK_SEQUENCE, name);
    }

    @Override
    public long[] genClusterUniqueLongIdBatch(String name, int batchSize) {
        return this.idGenerator.genClusterUniqueLongIdBatch(Const.ZK_SEQUENCE, name, batchSize);
    }

    @Override
    public int[] genClusterUniqueIntIdBatch(String name, int batchSize) {
        return this.idGenerator.genClusterUniqueIntIdBatch(Const.ZK_SEQUENCE, name, batchSize);
    }

    @Override
    public IIdGenerator getIdGenerator() {
        return idGenerator;
    }

    @Override
    public Lock createLock(String lockName) {
        return this.dbCluster.createLock(lockName);
    }

    @Override
    public void setIdGenerator(IIdGenerator idGenerator) {
        if (idGenerator == null) {
            throw new IllegalArgumentException("参数错误, 参数不能为空");
        }
        this.idGenerator = idGenerator;
    }

    @Override
    public IQuery createQuery() {
        IQuery query = new DefaultQueryImpl();
        return query;
    }

    public EnumDB getEnumDb() {
        return enumDb;
    }

    @Override
    public void setEnumDb(EnumDB enumDb) {
        if (enumDb == null) {
            throw new IllegalArgumentException("参数错误, 参数不能为空");
        }
        this.enumDb = enumDb;
    }

    @Override
    public void destroy() {
        // close database cluster.
        try {
            this.dbCluster.shutdown();
        } catch (DBClusterException e) {
            throw new RuntimeException(e);
        }

    }

    public EnumSyncAction getSyncAction() {
        return syncAction;
    }

    @Override
    public void setSyncAction(EnumSyncAction syncAction) {
        this.syncAction = syncAction;
    }

    public String getScanPackage() {
        return scanPackage;
    }

    @Override
    public void setScanPackage(String scanPackage) {
        if (StringUtils.isBlank(scanPackage)) {
            throw new IllegalArgumentException("参数错误，参数不能为空");
        }

        this.scanPackage = scanPackage;
    }

}
