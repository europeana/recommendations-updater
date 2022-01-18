package eu.europeana.api.recommend.updater.model.embeddings;

import java.io.Serializable;

@SuppressWarnings("java:S2384") // we'll ignore creating copies of field values to improve efficiency
public class EmbeddingRequestData implements Serializable {

    private static final long serialVersionUID = 4063496522104140402L;

    private final EmbeddingRecord[] records;

    public EmbeddingRequestData(EmbeddingRecord[] records) {
        this.records = records;
    }

    public EmbeddingRecord[] getRecords() {
        return records;
    }

    /**
     * If set to 1 then 300-dimensional vectors are returned, but we need the original laser embedding vectors with
     * dimension 1024 so should be set to 0
     */
    public int getReduce() {
        return 0;
    }

}
