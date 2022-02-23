package eu.europeana.api.recommend.updater.model.record;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Proxy object as defined in EDM, but only the fields we need in this application
 *
 * @author Patrick Ehlert
 */
@Document
public class Proxy  implements Serializable {

    private static final long serialVersionUID = 7919213706694351173L;

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

    public void setAbout(String about) {
        this.about = about;
    }

    public Map<String, List<String>> getDcCreator() {
        return dcCreator;
    }

    public void setDcCreator(Map<String, List<String>> dcCreator) {
        this.dcCreator = dcCreator;
    }

    public Map<String, List<String>> getDcContributor() {
        return dcContributor;
    }

    public void setDcContributor(Map<String, List<String>> dcContributor) {
        this.dcContributor = dcContributor;
    }

    public Map<String, List<String>> getDcDescription() {
        return dcDescription;
    }

    public void setDcDescription(Map<String, List<String>> dcDescription) {
        this.dcDescription = dcDescription;
    }

    public Map<String, List<String>> getDcFormat() {
        return dcFormat;
    }

    public void setDcFormat(Map<String, List<String>> dcFormat) {
        this.dcFormat = dcFormat;
    }

    public Map<String, List<String>> getDcSubject() {
        return dcSubject;
    }

    public void setDcSubject(Map<String, List<String>> dcSubject) {
        this.dcSubject = dcSubject;
    }

    public Map<String, List<String>> getDcTitle() {
        return dcTitle;
    }

    public void setDcTitle(Map<String, List<String>> dcTitle) {
        this.dcTitle = dcTitle;
    }

    public Map<String, List<String>> getDcType() {
        return dcType;
    }

    public void setDcType(Map<String, List<String>> dcType) {
        this.dcType = dcType;
    }

    public Map<String, List<String>> getDctermsAlternative() {
        return dctermsAlternative;
    }

    public void setDctermsAlternative(Map<String, List<String>> dctermsAlternative) {
        this.dctermsAlternative = dctermsAlternative;
    }

    public Map<String, List<String>> getDctermsCreated() {
        return dctermsCreated;
    }

    public void setDctermsCreated(Map<String, List<String>> dctermsCreated) {
        this.dctermsCreated = dctermsCreated;
    }

    public Map<String, List<String>> getDctermsIssued() {
        return dctermsIssued;
    }

    public void setDctermsIssued(Map<String, List<String>> dctermsIssued) {
        this.dctermsIssued = dctermsIssued;
    }

    public Map<String, List<String>> getDctermsMedium() {
        return dctermsMedium;
    }

    public void setDctermsMedium(Map<String, List<String>> dctermsMedium) {
        this.dctermsMedium = dctermsMedium;
    }

    public Map<String, List<String>> getDctermsTemporal() {
        return dctermsTemporal;
    }

    public void setDctermsTemporal(Map<String, List<String>> dctermsTemporal) {
        this.dctermsTemporal = dctermsTemporal;
    }

    public Map<String, List<String>> getDctermsSpatial() {
        return dctermsSpatial;
    }

    public void setDctermsSpatial(Map<String, List<String>> dctermsSpatial) {
        this.dctermsSpatial = dctermsSpatial;
    }

    public Map<String, List<String>> getEdmCurrentLocation() {
        return edmCurrentLocation;
    }

    public void setEdmCurrentLocation(Map<String, List<String>> edmCurrentLocation) {
        this.edmCurrentLocation = edmCurrentLocation;
    }

    public Map<String, List<String>> getEdmHasMet() {
        return edmHasMet;
    }

    public void setEdmHasMet(Map<String, List<String>> edmHasMet) {
        this.edmHasMet = edmHasMet;
    }
}
