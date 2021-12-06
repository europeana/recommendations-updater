package eu.europeana.api.recommend.updater.model.record;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;
import java.util.Map;

/**
 * Proxy object as defined in EDM, but only the fields we need in this application
 *
 * @author Patrick Ehlert
 */
@Document
public class Proxy {

    @Id
    private String about;
    private Map<String, List<String>> dcCreator;
    private Map<String, List<String>> dcContributor;
    private Map<String, List<String>> dcDescription;
    private Map<String, List<String>> dcFormat;
    private Map<String, List<String>> dcSubject;
    private Map<String, List<String>> dcTitle;
    private Map<String, List<String>> dcType;
    private Map<String, List<String>> dctermsAlternative;
    private Map<String, List<String>> dctermsCreated;
    private Map<String, List<String>> dctermsIssued;
    private Map<String, List<String>> dctermsMedium;
    private Map<String, List<String>> dctermsTemporal;
    private Map<String, List<String>> dctermsSpatial;
    private Map<String, List<String>> edmCurrentLocation;
    private Map<String, List<String>> edmHasMet;

    public String getAbout() {
        return about;
    }

    public Map<String, List<String>> getDcCreator() {
        return dcCreator;
    }

    public Map<String, List<String>> getDcContributor() {
        return dcContributor;
    }

    public Map<String, List<String>> getDcDescription() {
        return dcDescription;
    }

    public Map<String, List<String>> getDcFormat() {
        return dcFormat;
    }

    public Map<String, List<String>> getDcSubject() {
        return dcSubject;
    }

    public Map<String, List<String>> getDcTitle() {
        return dcTitle;
    }

    public Map<String, List<String>> getDcType() {
        return dcType;
    }

    public Map<String, List<String>> getDctermsAlternative() {
        return dctermsAlternative;
    }

    public Map<String, List<String>> getDctermsCreated() {
        return dctermsCreated;
    }

    public Map<String, List<String>> getDctermsIssued() {
        return dctermsIssued;
    }

    public Map<String, List<String>> getDctermsMedium() {
        return dctermsMedium;
    }

    public Map<String, List<String>> getDctermsTemporal() {
        return dctermsTemporal;
    }

    public Map<String, List<String>> getDctermsSpatial() {
        return dctermsSpatial;
    }

    public Map<String, List<String>> getEdmCurrentLocation() {
        return edmCurrentLocation;
    }

    public Map<String, List<String>> getEdmHasMet() {
        return edmHasMet;
    }

}
