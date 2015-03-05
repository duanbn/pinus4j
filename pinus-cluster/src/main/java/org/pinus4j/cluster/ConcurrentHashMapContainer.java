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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * a map container implement.
 *
 * @author duanbn
 * @since 1.0.0
 */
public class ConcurrentHashMapContainer<E> implements IContainer<E> {

	private final Map<String, E> map;

	ConcurrentHashMapContainer() {
		map = new ConcurrentHashMap<String, E>();
	}

	@Override
	public E find(String key) {
		E e = map.get(key);
		return e;
	}

	@Override
	public void add(String key, E e) {
		this.map.put(key, e);
	}

	@Override
	public Collection<E> values() {
		return this.map.values();
	}

}
