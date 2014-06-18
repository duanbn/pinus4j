package com.pinus.datalayer.jdbc;

import java.util.List;

import com.pinus.api.query.IQuery;
import com.pinus.cluster.DB;

/**
 * 分库分表引用，可以直接操作数据库提供增删改查功能.
 * 
 * @author duanbn
 * 
 */
public class FatDB<T> extends AbstractShardingQuery {

	private Class<T> clazz;

	private DB db;

	public List<T> loadByQuery(IQuery query) {
		List<T> result = null;

		if (isCacheAvailable(clazz)) {
			Number[] pkValues = selectPksByQuery(db, query, clazz);
			result = selectByPksWithCache(db, clazz, pkValues);
		} else {
			result = selectByQuery(db, query, clazz);
		}

		return result;
	}

	public Class<T> getClazz() {
		return clazz;
	}

	public void setClazz(Class<T> clazz) {
		this.clazz = clazz;
	}

	public DB getDb() {
		return db;
	}

	public void setDb(DB db) {
		this.db = db;
	}

}
