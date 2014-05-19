package com.pinus.exception;

/**
 * 数据库DDL相关异常.
 * 
 * @author duanbn
 */
public class DDLException extends Exception {

	private static final long serialVersionUID = 1L;

	public DDLException(String msg) {
        super(msg);
    }

    public DDLException(String msg, Exception e) {
        super(msg, e);
    }

    public DDLException(Exception e) {
        super(e);
    }

}
