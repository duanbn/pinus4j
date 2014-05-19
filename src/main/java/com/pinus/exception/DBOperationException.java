package com.pinus.exception;

/**
 * 数据库操作异常.
 *
 * @author duanbn
 */
public class DBOperationException extends RuntimeException {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DBOperationException(String message) {
        super(message);
    }

    public DBOperationException(Exception e) {
        super(e);
    }

    public DBOperationException(String message, Exception e) {
        super(message, e);
    }

}
