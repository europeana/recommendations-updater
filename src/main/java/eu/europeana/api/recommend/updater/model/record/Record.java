package eu.europeana.api.recommend.updater.model.record;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;

/**
 * Record object as defined in EDM, but only the fields we need in this application
 *
 * @author Patrick Ehlert
 */
@SuppressWarnings("java:S2384") // we'll ignore creating copies of field values to improve efficiency
@Document(collection = "record")
public class Record {

    @Id
    private ObjectId id;
    private String about;
    private List<Proxy> proxies;
    private List<Agent> agents;
    private List<Concept> concepts;
    private List<Place> places;
    private List<Timespan> timespans;
    private Date timestampCreated;
    private Date timestampUpdated;

    public String getAbout() {
        return about;
    }

    public List<Proxy> getProxies() {
        return proxies;
    }

    public List<Agent> getAgents() {
        return agents;
    }

    public List<Concept> getConcepts() {
        return concepts;
    }

    public List<Place> getPlaces() {
        return places;
    }

    public List<Timespan> getTimespans() {
        return timespans;
    }

    public Date getTimestampCreated() {
        return timestampCreated;
    }

    public Date getTimestampUpdated() {
        return timestampUpdated;
    }

}
