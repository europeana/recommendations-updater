package eu.europeana.api.recommend.updater.exception;

import eu.europeana.api.commons.error.EuropeanaApiException;

/**
 * Thrown when we there is a problem communicating with the Embeddings API
 */
public class EmbeddingsException extends EuropeanaApiException {

    /**
     * Initialise a new exception
     * @param msg error message
     * @param cause root cause exception
     */
    public EmbeddingsException(String msg, Throwable cause) {
        super(msg,cause);
    }

    /**
     * Initialise a new exception
     * @param msg error message
     */
    public EmbeddingsException(String msg) {
        super(msg);
    }

    /**
     * The stack trace for this exception is not logged
     * @return false
     */
    @Override
    public boolean doLogStacktrace() {
        return false;
    }
}
