package eu.europeana.api.recommend.updater.service.milvus;

import eu.europeana.api.recommend.updater.exception.MilvusStateException;
import eu.europeana.api.recommend.updater.model.record.Record;
import io.milvus.client.MilvusClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.ShowPartitionsResponse;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.partition.ShowPartitionsParam;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Patrick Ehlert
 */
public final class MilvusUtils {

    public static final int COLLECTION_DIMENSION = 300;
    public static final String RECORD_ID_KEY = "about";
    public static final String VECTOR_VALUE = "vector";
    private static final Logger LOG = LogManager.getLogger(MilvusUtils.class);

    private MilvusUtils() {
        // empty constructor to prevent initialization
    }

    /**
     * Checks the provided response and throws an error if it's not a successful response
     * @param response to check
     * @param errorMsg message in the thrown error
     * @return the original response (if response is success)
     */
    public static R checkResponse(R response, String errorMsg) {
        if (R.Status.Success.getCode() != response.getStatus()) {
            if (StringUtils.isBlank(errorMsg)) {
                errorMsg = response.getMessage();
            } else {
                errorMsg = errorMsg + ": " + response.getMessage();
            }
            throw new MilvusStateException(errorMsg, response.getException());
        }
        return response;
    }

    /**
     * Checks the provided response and throws an error if it's not a successful response
     * @param response to check
     * @return the original response (if response is success)
     */
    public static R checkResponse(R response) {
        return checkResponse(response, null);
    }

    /**
     * Creates a new collection and optionally an index using the provided MilvusClient
     * @param milvusClient
     * @param collectionName
     * @param indexName optional, if not null a index with the provided name is created
     */
    public static void createCollection(MilvusClient milvusClient, String collectionName, String collectionDescription, String indexName) {
        FieldType fieldType1 = FieldType.newBuilder()
                .withName(RECORD_ID_KEY)
                .withDescription("record id")
                .withDataType(DataType.VarChar)
                .withMaxLength(Record.MAX_RECORD_ID_LENGTH)
                .withPrimaryKey(true)
                .build();
        FieldType fieldType2 = FieldType.newBuilder()
                .withName(VECTOR_VALUE)
                .withDescription("record embedding")
                .withDataType(DataType.FloatVector)
                .withDimension(COLLECTION_DIMENSION)
                .build();

        LOG.info("Creating Milvus collection {}...", collectionName);
        LOG.info(checkResponse(milvusClient.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .withDescription(collectionDescription)
                .addFieldType(fieldType1)
                .addFieldType(fieldType2)
                .build()), "Error creating collection " + collectionName));

        if (StringUtils.isNotBlank(indexName)) {
            LOG.info("Creating index for collection {}...", collectionName);
            LOG.info(checkResponse(milvusClient.createIndex(CreateIndexParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withFieldName(VECTOR_VALUE)
                    .withIndexName(indexName)
                    .withIndexType(IndexType.IVF_SQ8) // copied from original code by Pangeanic
                    .withMetricType(MetricType.L2) // not sure, using L2 for now
                    .withExtraParam("{\"nlist\": 16384}") // copied from original code by Pangeanic
                    //.withSyncMode(Boolean.TRUE) // not sure, not setting for now
                    .build()), "Error creating index" + indexName));
        }
    }

    /**
     * Return a list of all partitions in a particular collection
     * @param milvusClient the client to use
     * @param collectionName the collection to query
     * @return list of partition names
     */
    public static List<String> getPartitions(MilvusClient milvusClient, String collectionName) {
        return getPartitions(milvusClient, collectionName, null);
    }

    /**
     * Return a list of all partitions in a particular collection
     * @param milvusClient the client to use
     * @param collectionName the collection to query
     * @param maxPartitions the maximum number of collections returned
     * @return list of partition names
     */
    public static List<String> getPartitions(MilvusClient milvusClient, String collectionName, Integer maxPartitions) {
        LOG.info("Listing partitions for collection {}...", collectionName);
        R<ShowPartitionsResponse> partitionsResponse = MilvusUtils.checkResponse(
                milvusClient.showPartitions(ShowPartitionsParam.newBuilder()
                        .withCollectionName(collectionName)
                        .build()), "Error listing partitions");
        List<String> result = new ArrayList<>();
        for (String partition : partitionsResponse.getData().getPartitionNamesList()) {
            LOG.info(partition);
            if (maxPartitions != null && result.size() >= maxPartitions) {
                LOG.info("Only listing first {} partitions", maxPartitions);
                break;
            }
        }
        return result;
    }

    /**
     * Delete a collection with the provided name, using the provided client
     * @param milvusClient
     * @param collectionName
     */
    public static void deleteCollection(MilvusClient milvusClient, String collectionName) {
//        LOG.info("Deleting index for collection {}");
//        R<RpcStatus> responseIndex = checkResponse(milvusClient.dropIndex(DropIndexParam.newBuilder()
//                .withCollectionName(TEST_COLLECTION)
//                .withIndexName(TEST_COLLECTION + INDEX_SUFFIX)
//                .build());
//        LOG.info(responseIndex);

        LOG.info("Dropping Milvus collection {}...", collectionName);
        LOG.info(checkResponse(milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .build())));
    }
}
