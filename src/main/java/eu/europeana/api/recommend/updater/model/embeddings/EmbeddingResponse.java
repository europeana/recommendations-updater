package eu.europeana.api.recommend.updater.model.embeddings;

import java.io.Serializable;

/**
 * Response sent back by Embeddings API
 *
 * @author Patrick Ehlert
 */
public class EmbeddingResponse implements Serializable {

    private static final long serialVersionUID = -4582261765664352355L;

    private RecordVectors[] data;
    private String status;

    public RecordVectors[] getData() {
        return data;
    }

    public String getStatus() {
        return status;
    }

}
