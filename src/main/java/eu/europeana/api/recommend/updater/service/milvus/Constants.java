package eu.europeana.api.recommend.updater.service.milvus;

import io.milvus.param.MetricType;

/**
 * Constants used in the generated collection or index
 */
public final class Constants {

    public static final String RECORD_ID_FIELD_NAME = "about";
    public static final String VECTOR_FIELD_NAME = "vector";
    public static final int VECTOR_DIMENSION = 300;
    public static final MetricType INDEX_METRIC_TYPE = MetricType.L2;

    public static final String MILVUS_SCORE_FIELD_NAME = "distance";

    private Constants() {
        // empty constructor to prevent initializiation
    }
}
