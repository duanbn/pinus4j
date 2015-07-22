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

import javax.transaction.TransactionManager;

import org.pinus4j.cache.IPrimaryCache;
import org.pinus4j.cache.ISecondCache;
import org.pinus4j.cluster.IDBCluster;

/**
 * base data query interface.
 *
 * @author duanbn
 * @since 0.7.1
 */
public interface IDataQuery {

    /**
	 *  set db cluster.
	 */
	public void setDBCluster(IDBCluster dbCluster);

    /**
     * get db cluster.
     */
    public IDBCluster getDBCluster();

    /**
	 * set primary cache.
	 */
	public void setPrimaryCache(IPrimaryCache primaryCache);

    /**
     * get second cache.
     */
    public IPrimaryCache getPrimaryCache();

	/**
	 * set second cache.
	 * 
	 * @param secondCache
	 */
	public void setSecondCache(ISecondCache secondCache);

    /**
     * get second cache.
     */
    public ISecondCache getSecondCache();
    
    /**
     * set transaction manager.
     * @param txManager
     */
    public void setTransactionManager(TransactionManager txManager);
    
    /**
     * get transaction manster.
     * @return
     */
    public TransactionManager getTransactionManager();
    
}
