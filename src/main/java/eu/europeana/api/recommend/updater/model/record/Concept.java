package eu.europeana.api.recommend.updater.model.record;

import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Concept object as defined in EDM, but only the fields we need in this application
 *
 * @author Patrick Ehlert
 */
@Document
public class Concept extends Entity {

    private static final long serialVersionUID = -551746464227652015L;

}
