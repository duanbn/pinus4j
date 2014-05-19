package com.pinus.exception;

/**
 * 缓存相关异常.
 *
 * @author duanbn
 */
public class DBCacheException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DBCacheException(String message) {
        super(message);
    }

    public DBCacheException(Exception e) {
        super(e);
    }

    public DBCacheException(String message, Exception e) {
        super(message, e);
    }

}
