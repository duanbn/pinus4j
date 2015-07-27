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

package org.pinus4j.datalayer.query;

import java.util.List;
import java.util.Map;

import org.pinus4j.api.SQL;
import org.pinus4j.api.query.IQuery;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.cluster.enums.EnumDBMasterSlave;
import org.pinus4j.entity.meta.EntityPK;

/**
 * query sharding data.
 * 
 * @author duanbn
 * @since 1.1.1
 */
public interface IShardingQuery {

    Number getCount(Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

    Number getCount(IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

    Number getCountByQuery(IQuery query, Class<?> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

    Number getCountByQuery(IQuery query, IShardingKey<?> shardingKey, Class<?> clazz, boolean useCache,
                           EnumDBMasterSlave masterSlave);

    <T> T findByPk(EntityPK pk, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

    <T> T findByPk(EntityPK pk, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
                   EnumDBMasterSlave masterSlave);

    <T> List<T> findByPkList(List<EntityPK> pkList, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

    <T> List<T> findByPkList(List<EntityPK> pkList, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
                             EnumDBMasterSlave masterSlave);

    <T> T findOneByQuery(IQuery query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

    <T> T findOneByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
                         EnumDBMasterSlave masterSlave);

    <T> List<T> findByQuery(IQuery query, Class<T> clazz, boolean useCache, EnumDBMasterSlave masterSlave);

    <T> List<T> findByQuery(IQuery query, IShardingKey<?> shardingKey, Class<T> clazz, boolean useCache,
                            EnumDBMasterSlave masterSlave);

    List<Map<String, Object>> findBySql(SQL sql, EnumDBMasterSlave masterSlave);

    List<Map<String, Object>> findBySql(SQL sql, IShardingKey<?> shardingKey, EnumDBMasterSlave masterSlave);

}
