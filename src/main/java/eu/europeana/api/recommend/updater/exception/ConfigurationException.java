package eu.europeana.api.recommend.updater.exception;

import eu.europeana.api.commons.error.EuropeanaApiException;

/**
 * Thrown when there is a non-recoverable error in the application's configuration
 */
public class ConfigurationException extends EuropeanaApiException {

    /**
     * Initialise a new configurations exception
     * @param msg error message
     */
    public ConfigurationException(String msg) {
        super(msg);
    }

    /**
     * Initialise a new configurations exception
     * @param msg error message
     * @param t root cause exception
     */
    public ConfigurationException(String msg, Throwable t) {
        super(msg, t);
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
