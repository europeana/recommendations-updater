package eu.europeana.api.recommend.updater.exception;

import eu.europeana.api.commons.error.EuropeanaApiException;

/**
 * Thrown when there a problem retrieving the set list from Solr
 */
public class SolrException extends EuropeanaApiException {

    /**
     * Initialise a new configurations exception
     * @param msg error message
     */
    public SolrException(String msg, Throwable e) {
        super(msg, e);
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
