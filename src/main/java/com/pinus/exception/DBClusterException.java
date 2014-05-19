package com.pinus.exception;

/**
 * 集群相关异常.
 *
 * @author duanbn
 */
public class DBClusterException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DBClusterException(String message) {
        super(message);
    }

    public DBClusterException(Exception e) {
        super(e);
    }

    public DBClusterException(String message, Exception e) {
        super(message, e);
    }

}
