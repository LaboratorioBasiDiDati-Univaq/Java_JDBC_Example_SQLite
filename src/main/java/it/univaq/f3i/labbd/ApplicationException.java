package it.univaq.f3i.labbd;

/**
 *
 * @author Giuseppe Della Penna
 */
public class ApplicationException extends Exception {

    public ApplicationException(String message) {
        super(message);
    }

    public ApplicationException(String message, Throwable cause) {
        super(message, cause);
    }
    
}
