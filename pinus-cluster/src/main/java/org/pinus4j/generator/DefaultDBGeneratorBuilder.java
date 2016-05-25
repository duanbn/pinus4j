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

package org.pinus4j.generator;

import org.pinus4j.cluster.enums.EnumDB;
import org.pinus4j.cluster.enums.EnumSyncAction;
import org.pinus4j.generator.impl.DBMySqlGeneratorImpl;

/**
 * db generator builder implements.
 *
 * @author duanbn
 * @since 0.7.1
 */
public class DefaultDBGeneratorBuilder implements IDBGeneratorBuilder {

    /**
     * 同步数据表操作.
     */
    private EnumSyncAction syncAction = EnumSyncAction.CREATE;

    private DefaultDBGeneratorBuilder(EnumSyncAction syncAction) {
        this.syncAction = syncAction;
    }

    public static IDBGeneratorBuilder valueOf(EnumSyncAction syncAction) {
        IDBGeneratorBuilder builder = new DefaultDBGeneratorBuilder(syncAction);
        return builder;
    }

    @Override
    public IDBGenerator build(EnumDB enumDB) {
        IDBGenerator dbGenerator = null;

        switch (enumDB) {
            case MYSQL:
                dbGenerator = new DBMySqlGeneratorImpl();
                break;
            default:
                dbGenerator = new DBMySqlGeneratorImpl();
                break;
        }

        dbGenerator.setSyncAction(this.syncAction);

        return dbGenerator;
    }

    public EnumSyncAction getSyncAction() {
        return syncAction;
    }

    public void setSyncAction(EnumSyncAction syncAction) {
        this.syncAction = syncAction;
    }

}
