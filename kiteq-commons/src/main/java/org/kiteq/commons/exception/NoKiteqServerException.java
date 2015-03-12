package org.kiteq.commons.exception;

/**
 * luofucong at 2015-03-12.
 */
public class NoKiteqServerException extends Exception {

    public NoKiteqServerException(String topic) {
        super(topic);
    }
}
