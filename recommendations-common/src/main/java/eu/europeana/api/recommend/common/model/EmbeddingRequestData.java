package eu.europeana.api.recommend.common.model;

import java.io.Serializable;

@SuppressWarnings("java:S2384") // we'll ignore creating copies of field values to improve efficiency
public class EmbeddingRequestData implements Serializable {

    public static final int REDUCE = 1;

    private static final long serialVersionUID = 4063496522104140402L;

    private final EmbeddingRecord[] records;

    public EmbeddingRequestData(EmbeddingRecord[] records) {
        this.records = records;
    }

    public EmbeddingRecord[] getRecords() {
        return records;
    }

    /**
     * If set to 1 then 300-dimensional vectors are returned, otherwise 1000.
     * We need vectors of size 300.
     */
    public int getReduce() {
        return REDUCE;
    }

}
