package eu.europeana.api.recommend.updater.exception;

/**
 * Thrown when there is a non-recoverable error in the state of Milvus (e.g. non-empty target collection when doing
 * a full-update, or collection not found when doing a partial update).
 */
public class MilvusStateException extends RuntimeException {

    /**
     * Initialise a new milvus state exception
     * @param msg error message
     */
    public MilvusStateException(String msg) {
        super(msg);
    }

}
