package eu.europeana.api.recommend.updater.exception;

import eu.europeana.api.commons.error.EuropeanaApiException;

/**
 * Thrown when there is a non-recoverable error in the state of Milvus (e.g. non-empty target collection when doing
 * a full-update, or collection not found when doing a partial update).
 */
public class MilvusStateException extends EuropeanaApiException {

    /**
     * Initialise a new configurations exception
     * @param msg error message
     */
    public MilvusStateException(String msg) {
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
