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

import java.util.List;

import org.pinus4j.api.query.IQuery;
import org.pinus4j.api.query.impl.Condition;
import org.pinus4j.cluster.beans.IShardingKey;
import org.pinus4j.entity.DefaultEntityMetaManager;
import org.pinus4j.entity.IEntityMetaManager;
import org.pinus4j.entity.meta.EntityPK;
import org.pinus4j.entity.meta.PKName;
import org.pinus4j.entity.meta.PKValue;

import com.google.common.collect.Lists;

/**
 * 继承此对象的Entity对象会具备save, update, saveOrUpdate, remove方法.
 * 
 * @author duanbn
 */
public abstract class FashionEntity {

    private IEntityMetaManager entityMetaManager = DefaultEntityMetaManager.getInstance();
    
    public void load() {
        PinusClient pinusClient = DefaultPinusClient.instance;
        pinusClient.load(this);
    }

    /**
     * 保存
     */
    public void save() {
        PinusClient pinusClient = DefaultPinusClient.instance;
        pinusClient.save(this);
    }

    /**
     * 更新
     */
    public void update() {
        PinusClient pinusClient = DefaultPinusClient.instance;
        pinusClient.update(this);
    }

    /**
     * 如果存在则更新，否则保存.
     */
    public void saveOrUpdate() {
        Class<?> clazz = this.getClass();

        PinusClient pinusClient = DefaultPinusClient.instance;

        EntityPK entityPK = entityMetaManager.getEntityPK(this);

        IQuery<?> query = pinusClient.createQuery(clazz);
        List<Condition> orCond = Lists.newArrayList();
        PKName[] pkNames = entityPK.getPkNames();
        PKValue[] pkValues = entityPK.getPkValues();
        for (int i = 0; i < pkNames.length; i++) {
            orCond.add(Condition.eq(pkNames[i].getValue(), pkValues[i].getValue()));
        }
        query.and(Condition.or(orCond.toArray(new Condition[orCond.size()])));

        if (entityMetaManager.isShardingEntity(clazz)) {
            IShardingKey<?> sk = entityMetaManager.getShardingKey(this);
            query.setShardingKey(sk);
        }

        Object obj = query.load();

        if (obj != null) {
            update();
        } else {
            save();
        }
    }

    /**
     * 删除.
     */
    public void remove() {
        PinusClient pinusClient = DefaultPinusClient.instance;
        pinusClient.delete(this);
    }
}
