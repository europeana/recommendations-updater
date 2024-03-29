package eu.europeana.api.recommend.common;

import io.milvus.param.MetricType;

/**
 * Constants used in the generated Milvus collection or index
 * @author Patrick Ehlert
 */
public final class MilvusConstants {

    public static final String RECORD_ID_FIELD_NAME = "about";
    public static final String VECTOR_FIELD_NAME = "vector";
    public static final int VECTOR_DIMENSION = 300;
    public static final MetricType INDEX_METRIC_TYPE = MetricType.L2;
    public static final String MILVUS_SCORE_FIELD_NAME = "distance";

    private MilvusConstants() {
        // empty constructor to prevent initializiation
    }
}
