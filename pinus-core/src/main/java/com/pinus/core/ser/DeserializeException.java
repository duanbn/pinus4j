package com.pinus.core.ser;

/**
 * deserialize exception.
 *
 * @author duanbn
 */
public class DeserializeException extends Exception {
    
    public DeserializeException(String message) {
        super(message);
    }

    public DeserializeException(String message, Throwable cause) {
        super(message, cause);
    }

    public DeserializeException(Throwable cause) {
        super(cause);
    }

}
