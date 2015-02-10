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

import java.util.function.Supplier;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.pinus4j.transaction.enums.EnumTransactionIsolationLevel;

/**
 * best effort one pc transaction manager implements for jta.
 * 
 * @author duanbn
 * @since 1.1.0
 */
public class BestEffortsOnePCJtaTransactionManager implements TransactionManager {

	private static final TransactionManager instance = new BestEffortsOnePCJtaTransactionManager();

	private static final ThreadLocal<EnumTransactionIsolationLevel> txLevelLocal = ThreadLocal
			.withInitial(new Supplier<EnumTransactionIsolationLevel>() {
				@Override
				public EnumTransactionIsolationLevel get() {
					return EnumTransactionIsolationLevel.READ_COMMITTED;
				}
			});

	private static final ThreadLocal<Transaction> txLocal = new ThreadLocal<Transaction>();

	/**
	 * get singleton instance.
	 * 
	 * @return
	 */
	public static TransactionManager getInstance() {
		return instance;
	}

	public void setTransactionIsolationLevel(EnumTransactionIsolationLevel txLevel) {
		txLevelLocal.set(txLevel);
	}

	public EnumTransactionIsolationLevel getTransactionIsolationLevel() {
		return txLevelLocal.get();
	}

	@Override
	public void begin() throws NotSupportedException, SystemException {
		Transaction tx = txLocal.get();

		if (tx == null) {
			synchronized (this) {
				if (tx == null) {
					tx = new LocalTransaction();
					((LocalTransaction) tx).setIsolationLevel(getTransactionIsolationLevel());
					txLocal.set(tx);
				}
			}
		}

		txLocal.set(tx);
	}

	@Override
	public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
			SecurityException, IllegalStateException, SystemException {
		try {
			txLocal.get().commit();
		} finally {
			// commit finished, remove transaction object from current thread.
			txLocal.remove();
		}
	}

	@Override
	public int getStatus() throws SystemException {
		if (txLocal.get() != null) {
			return txLocal.get().getStatus();
		} else {
			return Status.STATUS_NO_TRANSACTION;
		}
	}

	@Override
	public Transaction getTransaction() throws SystemException {
		return txLocal.get();
	}

	@Override
	public void resume(Transaction arg0) throws InvalidTransactionException, IllegalStateException, SystemException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void rollback() throws IllegalStateException, SecurityException, SystemException {
		try {
			txLocal.get().rollback();
		} finally {
			txLocal.remove();
		}
	}

	@Override
	public void setRollbackOnly() throws IllegalStateException, SystemException {
		txLocal.get().setRollbackOnly();
	}

	@Override
	public void setTransactionTimeout(int arg0) throws SystemException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Transaction suspend() throws SystemException {
		throw new UnsupportedOperationException();
	}

}
