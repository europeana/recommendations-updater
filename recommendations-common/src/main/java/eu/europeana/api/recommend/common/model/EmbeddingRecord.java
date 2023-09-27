package eu.europeana.api.recommend.common.model;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Stores CHO record data that is posted to Embeddings API
 *
 * @author Patrick Ehlert
 */
@SuppressWarnings("java:S2384") // we'll ignore creating copies of field values to improve efficiency
public class EmbeddingRecord  implements Serializable {

    private static final long serialVersionUID = 8981198216794631059L;

    @NotNull
    private String id;
    @NotNull
    private String[] title;
    private String[] description;
    private String[] creator;
    private String[] tags;
    private String[] places;
    private String[] times;

    /**
     * Create a new EmbeddingRecord object to send to Embeddings API
     * @param about the id of the record, mandatory
     * @param title title of the CHO, mandatory
     * @param description description of the CHO
     * @param creator the creator of the CHO
     * @param tags array of related tags
     * @param places array of related places
     * @param times array of related time frames
     */
    public EmbeddingRecord(String about, String[] title, String[] description, String[] creator, String[] tags, String[] places, String[] times) {
        this.id = about;
        this.title = title;
        this.description = description;
        this.creator = creator;
        this.tags = tags;
        this.places = places;
        this.times = times;
    }

    private EmbeddingRecord() {
        // empty constructor for Jackson serialization
    }

    public String getId() {
        return id;
    }

    public String[] getTitle() {
        return title;
    }

    public String[] getDescription() {
        return description;
    }

    public String[] getCreator() {
        return creator;
    }

    public String[] getTags() {
        return tags;
    }

    public String[] getPlaces() {
        return places;
    }

    public String[] getTimes() {
        return times;
    }

    @Override
    public String toString() {
        return "EmbeddingRecord{" +
                "id='" + id + '\'' +
                ", title=" + Arrays.toString(title) +
                ", description=" + Arrays.toString(description) +
                ", creator=" + Arrays.toString(creator) +
                ", tags=" + Arrays.toString(tags) +
                ", places=" + Arrays.toString(places) +
                ", times=" + Arrays.toString(times) +
                '}';
    }
}
