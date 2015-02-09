package org.pinus4j.transaction.impl;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

/**
 * user transaction implement.
 * 
 * @author duanbn
 * @since 1.1.0
 */
public class UserTransactionImpl implements UserTransaction {

	@Override
	public void begin() throws NotSupportedException, SystemException {
		BestEffortsOnePCJtaTransactionManager.getInstance().begin();
	}

	@Override
	public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
			SecurityException, IllegalStateException, SystemException {
		BestEffortsOnePCJtaTransactionManager.getInstance().commit();
	}

	@Override
	public int getStatus() throws SystemException {
		return BestEffortsOnePCJtaTransactionManager.getInstance().getStatus();
	}

	@Override
	public void rollback() throws IllegalStateException, SecurityException, SystemException {
		BestEffortsOnePCJtaTransactionManager.getInstance().rollback();
	}

	@Override
	public void setRollbackOnly() throws IllegalStateException, SystemException {
		BestEffortsOnePCJtaTransactionManager.getInstance().setRollbackOnly();
	}

	@Override
	public void setTransactionTimeout(int arg0) throws SystemException {
		BestEffortsOnePCJtaTransactionManager.getInstance().setTransactionTimeout(arg0);
	}

}
