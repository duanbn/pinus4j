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

package org.pinus.generator;

import org.pinus.api.enums.EnumDB;
import org.pinus.api.enums.EnumSyncAction;

/**
 * db generator builder interface.
 *
 * @author duanbn
 * @since 0.7.1
 */
public interface IDBGeneratorBuilder {

    /**
     * set sync action.
     */
    public void setSyncAction(EnumSyncAction syncAction);

    /**
     * set db catalog.
     */
    public void setDBCatalog(EnumDB enumDb);

    /**
     * create new db generator instance.
     */
    public IDBGenerator build();

}
