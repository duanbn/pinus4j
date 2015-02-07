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

import org.pinus4j.exceptions.TransactionException;
import org.pinus4j.transaction.DefaultTransaction;
import org.pinus4j.transaction.ITransaction;
import org.pinus4j.transaction.ITransactionManager;

/**
 * default transaction manager implements.
 *
 * @author duanbn
 * @since 1.1.0
 */
public class BestEffortsOnePcTransactionManager implements ITransactionManager {

    /**
     * hold current thread transaction object.
     */
    private final ThreadLocal<ITransaction> transactionLocal = new ThreadLocal<ITransaction>();

    @Override
    public ITransaction beginTransaction() {
        ITransaction transaction = transactionLocal.get();

        if (transaction == null) {
            synchronized (this) {
                if (transaction == null) {
                    transaction = new DefaultTransaction();
                    transactionLocal.set(transaction);
                }
            }
        }

        return transaction;
    }

    @Override
    public void beginDoCommit() throws TransactionException {
        // TODO:
    }

    @Override
    public void endDoCommit() {
        // commit finished, remove transaction object from current thread.
        transactionLocal.remove();
    }

}
