package eu.europeana.api.recommend.updater.exception;

import eu.europeana.api.commons.error.EuropeanaGlobalExceptionHandler;
import org.springframework.web.bind.annotation.ControllerAdvice;

/**
 * Basic error processing (whether to log an error or not) is done in the EuropeanaGlobalExceptionHandler, but
 * we can add more error handling here
 */
@ControllerAdvice
public class GlobalExceptionHandler extends EuropeanaGlobalExceptionHandler {

}
