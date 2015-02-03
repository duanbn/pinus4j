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

/**
 * container interface.
 *
 * @author duanbn
 * @since 1.0.0
 */
public interface IContainer<E> {

    /**
     * find a element from this container.
     *
     * @return if can find will be return null.
     */
    public E find(String key);

    /**
     * add element to this container.
     *
     * @param key a key.
     * @param e element will be puted to container.
     */
    public void add(String key, E e);

    /**
     * get collection of this container's value.
     *
     * @return collection of container's value.
     */
    public Collection<E> values();

}
