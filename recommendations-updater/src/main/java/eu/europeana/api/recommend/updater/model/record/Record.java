package eu.europeana.api.recommend.updater.model.record;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * Record object as defined in EDM, but only the fields we need in this application
 *
 * @author Patrick Ehlert
 */
@SuppressWarnings("java:S2384") // we'll ignore creating copies of field values to improve efficiency
@Document(collection = "record")
public class Record implements Serializable {

    private static final long serialVersionUID = 7041954999800826470L;


    @Id
    private ObjectId id;
    private String about;
    private List<Proxy> proxies;
    private List<Agent> agents;
    private List<Concept> concepts;
    private List<Place> places;
    private List<Timespan> timespans;
    private Date timestampUpdated;

    public String getMongoId() {
        return id.toString();
    }

    public String getAbout() {
        return about;
    }

    public void setAbout(String about) {
        this.about = about;
    }

    public List<Proxy> getProxies() {
        return proxies;
    }

    public void setProxies(List<Proxy> proxies) {
        this.proxies = proxies;
    }

    public List<Agent> getAgents() {
        return agents;
    }

    public void setAgents(List<Agent> agents) {
        this.agents = agents;
    }

    public List<Concept> getConcepts() {
        return concepts;
    }

    public void setConcepts(List<Concept> concepts) {
        this.concepts = concepts;
    }

    public List<Place> getPlaces() {
        return places;
    }

    public void setPlaces(List<Place> places) {
        this.places = places;
    }

    public List<Timespan> getTimespans() {
        return timespans;
    }

    public void setTimespans(List<Timespan> timespans) {
        this.timespans = timespans;
    }

    public Date getTimestampUpdated() {
        return timestampUpdated;
    }

    public void setTimestampUpdated(Date timestampUpdated) {
        this.timestampUpdated = timestampUpdated;
    }
}
