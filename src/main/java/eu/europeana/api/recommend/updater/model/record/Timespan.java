package eu.europeana.api.recommend.updater.model.record;

import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Timespan object as defined in EDM, but only the fields we need in this application
 *
 * @author Patrick Ehlert
 */
@Document
public class Timespan extends Entity {

    private static final long serialVersionUID = 3273995406944389789L;

}
