package com.pinus.core.ser;

/**
 * serialize exception.
 *
 * @author duanbn 
 */
public class SerializeException extends Exception {
    
    public SerializeException(String message) {
        super(message);
    }

    public SerializeException(String message, Throwable cause) {
        super(message, cause);
    }

    public SerializeException(Throwable cause) {
        super(cause);
    }

}
