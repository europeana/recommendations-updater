package eu.europeana.api.recommend.updater.model.record;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Common fields for all entities defined in EDM, but only the fields we need in this application
 *
 * @author Patrick Ehlert
 */
public class Entity  implements Serializable {

    private static final long serialVersionUID = 932678045340604732L;

    private String about;
    private Map<String, List<String>> prefLabel;

    public String getAbout() {
        return about;
    }

    public void setAbout(String about) {
        this.about = about;
    }

    public Map<String, List<String>> getPrefLabel() {
        return prefLabel;
    }

    public void setPrefLabel(Map<String, List<String>> prefLabel) {
        this.prefLabel = prefLabel;
    }

}
