package org.pinus4j.exceptions;

/**
 * codec exception.
 *
 * @author duanbingnan
 */
public class CodecException extends Exception {
    
    public CodecException(String message) {
        super(message);
    }

    public CodecException(String message, Throwable cause) {
        super(message, cause);
    }

    public CodecException(Throwable cause) {
        super(cause);
    }

}
