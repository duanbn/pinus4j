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

package org.pinus4j.transaction;

import java.util.LinkedHashMap;
import java.util.Map;

import org.pinus4j.cluster.resources.IDBResource;
import org.pinus4j.cluster.resources.IResourceId;

/**
 * default transaction impelemnt.
 *
 * @author duanbn
 * @since 1.1.0
 */
public class LocalTransaction implements ITransaction {

	private Map<IResourceId, IDBResource> txRes = new LinkedHashMap<IResourceId, IDBResource>();

	@Override
	public void appendResource(IDBResource dbResource) {
		IResourceId resId = dbResource.getId();

		if (txRes.get(resId) == null) {
			synchronized (txRes) {
				if (txRes.get(resId) == null) {
					txRes.put(resId, dbResource);
				}
			}
		}

	}

	/**
	 * do commit.
	 */
	public void commit() {
		for (IDBResource dbResource : txRes.values()) {
			dbResource.commit();
		}
	}

	/**
	 * do rollback.
	 */
	public void rollback() {
		for (IDBResource dbResource : txRes.values()) {
			dbResource.rollback();
		}
	}

}
