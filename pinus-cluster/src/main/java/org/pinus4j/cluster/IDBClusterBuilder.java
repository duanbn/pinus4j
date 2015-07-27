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
