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

package org.pinus.api;

import org.pinus.util.ReflectUtil;

/**
 * 继承此对象的Entity对象会具备save, update, saveOrUpdate, remove方法.
 * 
 * @author duanbn
 */
public abstract class FashionEntity {

	/**
	 * 保存
	 */
	public Number save() {
		Number pk = null;

		IShardingStorageClient storageClient = ShardingStorageClientImpl.instance;
		if (ReflectUtil.isShardingEntity(this.getClass())) {
			pk = storageClient.save(this);
		} else {
			pk = storageClient.globalSave(this);
		}

		return pk;
	}

	/**
	 * 更新
	 */
	public void update() {
		IShardingStorageClient storageClient = ShardingStorageClientImpl.instance;
		if (ReflectUtil.isShardingEntity(this.getClass())) {
			storageClient.update(this);
		} else {
			storageClient.globalUpdate(this);
		}
	}

	/**
	 * 如果存在则更新，否则保存.
	 */
	public Number saveOrUpdate() {
		Number pk = ReflectUtil.getPkValue(this);
		if (pk.intValue() == 0) {
			return save();
		}

		Object obj = null;

		Class<?> clazz = this.getClass();
		String clusterName = ReflectUtil.getClusterName(clazz);
		IShardingStorageClient storageClient = ShardingStorageClientImpl.instance;
		if (ReflectUtil.isShardingEntity(clazz)) {
			Object shardingValue = ReflectUtil.getShardingValue(this);
			IShardingKey<Object> sk = new ShardingKey<Object>(clusterName, shardingValue);
			obj = storageClient.findByPk(pk, sk, clazz);
		} else {
			obj = storageClient.findGlobalByPk(pk, clusterName, clazz);
		}

		if (obj != null) {
			update();
			return pk;
		} else {
			return save();
		}
	}

	/**
	 * 删除.
	 */
	public void remove() {
		Number pk = ReflectUtil.getPkValue(this);

		if (pk.intValue() == 0) {
			return;
		}

		Class<?> clazz = this.getClass();
		String clusterName = ReflectUtil.getClusterName(clazz);
		IShardingStorageClient storageClient = ShardingStorageClientImpl.instance;
		if (ReflectUtil.isShardingEntity(this.getClass())) {
			Object shardingValue = ReflectUtil.getShardingValue(this);
			IShardingKey<Object> sk = new ShardingKey<Object>(clusterName, shardingValue);
			storageClient.removeByPk(pk, sk, clazz);
		} else {
			storageClient.globalRemoveByPk(pk, clazz, clusterName);
		}
	}
}
