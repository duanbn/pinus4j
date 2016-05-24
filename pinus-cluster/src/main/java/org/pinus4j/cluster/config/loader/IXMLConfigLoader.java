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

package org.pinus4j.cluster.config.loader;

import org.pinus4j.exceptions.LoadConfigException;
import org.w3c.dom.Node;

/**
 * 类IConfigLoader.java的实现描述：TODO 类实现描述
 * 
 * @author duanbn May 24, 2016 10:47:06 AM
 */
public interface IXMLConfigLoader<E> {

    E load(Node xmlNode) throws LoadConfigException;

}
