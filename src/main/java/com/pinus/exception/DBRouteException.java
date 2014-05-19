package com.pinus.exception;

/**
 * 集群相关异常.
 *
 * @author duanbn
 */
public class DBRouteException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DBRouteException(String message) {
        super(message);
    }

    public DBRouteException(Exception e) {
        super(e);
    }

    public DBRouteException(String message, Exception e) {
        super(message, e);
    }

}
