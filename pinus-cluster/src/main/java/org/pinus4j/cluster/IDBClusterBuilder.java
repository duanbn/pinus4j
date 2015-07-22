package org.pinus4j.cluster;

import org.pinus4j.cluster.enums.EnumDB;
import org.pinus4j.cluster.enums.EnumSyncAction;

/**
 * for build database cluster instance.
 * 
 * @author duanbn
 *
 */
public interface IDBClusterBuilder {

	/**
	 * build a instance.
	 * 
	 * @return
	 */
	IDBCluster build();

	/**
	 * set db type.
	 * 
	 * @param enumDb
	 */
	void setDbType(EnumDB enumDb);

	/**
	 * set db sync action.
	 * 
	 * @param syncAction
	 */
	void setSyncAction(EnumSyncAction syncAction);

	/**
	 * set scan entity package.
	 * 
	 * @param scanPackage
	 */
	void setScanPackage(String scanPackage);

}
