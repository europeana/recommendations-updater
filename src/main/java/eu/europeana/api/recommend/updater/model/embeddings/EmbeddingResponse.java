package eu.europeana.api.recommend.updater.model.embeddings;

/**
 * Response sent back by Embeddings API
 *
 * @author Patrick Ehlert
 */
public class EmbeddingResponse {

    private RecordVectors[] data;
    private String status;

    public RecordVectors[] getData() {
        return data;
    }

    public String getStatus() {
        return status;
    }

}
