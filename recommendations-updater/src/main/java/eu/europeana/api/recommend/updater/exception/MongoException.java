package eu.europeana.api.recommend.updater.exception;

/**
 * Thrown when there is an error reading from Mongo
 */
public class MongoException extends RuntimeException {

    /**
     * Initialise a Mongo exception
     * @param msg error message
     * @param t root cause exception
     */
    public MongoException(String msg, Throwable t) {
        super(msg, t);
    }

}
