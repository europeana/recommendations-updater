package eu.europeana.api.recommend.updater.exception;

/**
 * Thrown when there is a non-recoverable error in the state of LMDB (e.g. non-empty database when doing
 * a full-update, or collection not found when doing a partial update).
 */
public class LmdbStateException extends RuntimeException {

    /**
     * Initialise a new lmdb state exception
     * @param msg error message
     */
    public LmdbStateException(String msg) {
        super(msg);
    }

    /**
     * Initialise a new lmdb state exception with root cause
     * @param msg error message
     * @param t root cause exception
     */
    public LmdbStateException(String msg, Throwable t) {
        super(msg, t);
    }

}
