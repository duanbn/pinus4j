package com.pinus.exception;

/**
 * 加载配置信息异常.
 *
 * @author duanbn
 */
public class LoadConfigException extends Exception {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public LoadConfigException(String message) {
        super(message);
    }

    public LoadConfigException(Exception e) {
        super(e);
    }

    public LoadConfigException(String message, Exception e) {
        super(message, e);
    }

}
