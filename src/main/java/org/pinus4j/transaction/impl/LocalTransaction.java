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

import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.cluster.resources.IResourceId;
import org.pinus4j.transaction.ITransaction;

/**
 * default transaction impelemnt.
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
     * db resource will only close connection.
     */
    private Map<IResourceId, IDBResource> txResReadOnly = new LinkedHashMap<IResourceId, IDBResource>();

	@Override
	public void append(IDBResource dbResource) {
		IResourceId resId = dbResource.getId();

		if (txRes.get(resId) == null) {
			synchronized (txRes) {
				if (txRes.get(resId) == null) {
					txRes.put(resId, dbResource);
				}
			}
		}
	}

    @Override
    public void appendReadOnly(IDBResource dbResource) {
        IResourceId resId = dbResource.getId();

		if (txResReadOnly.get(resId) == null) {
			synchronized (txResReadOnly) {
				if (txResReadOnly.get(resId) == null) {
					txResReadOnly.put(resId, dbResource);
				}
			}
		}
    }

	/**
	 * do commit.
	 */
	public void commit() {
        // do commit
		for (IDBResource dbResource : txRes.values()) {
			dbResource.commit();
            dbResource.close();
		}

        // close read only.
        for (IDBResource dbResource : txResReadOnly.values()) {
            dbResource.close();
        }
	}

	/**
	 * do rollback.
	 */
	public void rollback() {
        // do rollback.
		for (IDBResource dbResource : txRes.values()) {
			dbResource.rollback();
            dbResource.close();
		}

        // close read only.
        for (IDBResource dbResource : txResReadOnly.values()) {
            dbResource.close();
        }
	}

}
