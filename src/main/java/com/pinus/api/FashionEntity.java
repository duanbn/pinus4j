package com.pinus.api;

import com.pinus.util.ReflectUtil;

/**
 * 继承此对象的Entity对象会具备save, update, saveOrUpdate, remove方法.
 * 
 * @author duanbn
 */
public abstract class FashionEntity {

	/**
	 * sharding client ref.
	 */
	protected transient IShardingStorageClient storageClient;

	public FashionEntity() {
		// get sharding client ref from threadlocal.
		storageClient = IShardingStorageClient.storageClientHolder.get();
	}

	/**
	 * 保存
	 */
	public Number save() {
		Number pk = null;

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
		if (ReflectUtil.isShardingEntity(clazz)) {
			Object shardingValue = ReflectUtil.getShardingValue(this);
			IShardingKey<Object> sk = new ShardingKey<Object>(clusterName, shardingValue);
			obj = storageClient.findByPk(pk, sk, clazz);
		} else {
			obj = storageClient.findGlobalByPk(pk, clusterName, clazz);
		}

		if (obj != null) {
			update();
		} else {
			return save();
		}

		return null;
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
		if (ReflectUtil.isShardingEntity(this.getClass())) {
			Object shardingValue = ReflectUtil.getShardingValue(this);
			IShardingKey<Object> sk = new ShardingKey<Object>(clusterName, shardingValue);
			storageClient.removeByPk(pk, sk, clazz);
		} else {
			storageClient.globalRemoveByPk(pk, clazz, clusterName);
		}
	}
}
