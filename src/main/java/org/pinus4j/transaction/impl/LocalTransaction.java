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

package org.pinus4j.transaction.impl;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.xa.XAResource;

import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.cluster.resources.IResourceId;
import org.pinus4j.transaction.ITransaction;
import org.pinus4j.transaction.enums.EnumTransactionIsolationLevel;

/**
 * local transaction impelemnt.
 *
 * @author duanbn
 * @since 1.1.0
 */
public class LocalTransaction implements ITransaction {

	/**
	 * db resource will do commit or rollback.
	 */
	private Map<IResourceId, IDBResource> txRes = new LinkedHashMap<IResourceId, IDBResource>();

	/**
	 * isolation level of transaction.
	 */
	private EnumTransactionIsolationLevel txLevel;

	private int status;

	@Override
	public void setIsolationLevel(EnumTransactionIsolationLevel txLevel) {
		this.txLevel = txLevel;
	}

	/**
	 * do commit.
	 */
	@Override
	public void commit() {
		// do commit
		for (IDBResource dbResource : txRes.values()) {
			dbResource.commit();
			dbResource.close();
		}
	}

	/**
	 * do rollback.
	 */
	@Override
	public void rollback() {
		// do rollback.
		for (IDBResource dbResource : txRes.values()) {
			dbResource.rollback();
			dbResource.close();
		}
	}

	// jta implements.
	@Override
	public boolean delistResource(XAResource xaResource, int arg1) throws IllegalStateException, SystemException {
		IDBResource dbResource = (IDBResource) xaResource;
		IResourceId resId = dbResource.getId();

		txRes.remove(resId);
		return true;
	}

	@Override
	public boolean enlistResource(XAResource xaResource) throws RollbackException, IllegalStateException,
			SystemException {
		IDBResource dbResource = (IDBResource) xaResource;
		IResourceId resId = dbResource.getId();

		if (txRes.get(resId) == null) {
			synchronized (txRes) {
				if (txRes.get(resId) == null) {
					dbResource.setTransactionIsolationLevel(txLevel);
					txRes.put(resId, dbResource);
				}
			}
		}

		return true;
	}

	@Override
	public int getStatus() throws SystemException {
		return status;
	}

	@Override
	public void registerSynchronization(Synchronization arg0) throws RollbackException, IllegalStateException,
			SystemException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setRollbackOnly() throws IllegalStateException, SystemException {
		// do nothing...
	}

}
