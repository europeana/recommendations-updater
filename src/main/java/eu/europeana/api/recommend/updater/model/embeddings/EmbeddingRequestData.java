package eu.europeana.api.recommend.updater.model.embeddings;

@SuppressWarnings("java:S2384") // we'll ignore creating copies of field values to improve efficiency
public class EmbeddingRequestData {

    private final EmbeddingRecord[] records;

    public EmbeddingRequestData(EmbeddingRecord[] records) {
        this.records = records;
    }

    public EmbeddingRecord[] getRecords() {
        return records;
    }

}
