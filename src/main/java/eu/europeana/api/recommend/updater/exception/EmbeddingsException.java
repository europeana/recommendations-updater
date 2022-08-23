package eu.europeana.api.recommend.updater.exception;

import eu.europeana.api.commons.error.EuropeanaApiException;

/**
 * Thrown when we can't connect to Embeddings API
 */
public class EmbeddingsException extends EuropeanaApiException {

    /**
     * Initialise a new configurations exception
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
