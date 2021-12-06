package eu.europeana.api.recommend.updater.model.embeddings;

import javax.validation.constraints.NotNull;
import java.util.Arrays;

/**
 * Stores CHO record data that is posted to Embeddings API
 *
 * @author Patrick Ehlert
 */
@SuppressWarnings("java:S2384") // we'll ignore creating copies of field values to improve efficiency
public class EmbeddingRecord {

    @NotNull
    private final String id;
    @NotNull
    private final String[] title;
    private final String[] description;
    private final String[] creator;
    private final String[] tags;
    private final String[] places;
    private final String[] times;

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
