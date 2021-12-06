package eu.europeana.api.recommend.updater.model.record;

import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Agent object as defined in EDM, but only the fields we need in this application
 *
 * @author Patrick Ehlert
 */
@Document
public class Agent extends Entity {
}
