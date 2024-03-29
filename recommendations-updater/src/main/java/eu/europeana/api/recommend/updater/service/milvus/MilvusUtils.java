package eu.europeana.api.recommend.updater.service.milvus;

import eu.europeana.api.recommend.common.MilvusConstants;
import eu.europeana.api.recommend.common.RecordId;
import eu.europeana.api.recommend.updater.exception.MilvusStateException;
import io.milvus.client.MilvusClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.grpc.ShowPartitionsResponse;
import io.milvus.param.IndexType;
import io.milvus.param.R;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.GetCollectionStatisticsParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.partition.ShowPartitionsParam;
import io.milvus.response.GetCollStatResponseWrapper;
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
                .withName(MilvusConstants.RECORD_ID_FIELD_NAME)
                .withDescription("record id")
                .withDataType(DataType.VarChar)
                .withMaxLength(RecordId.MAX_SIZE)
                .withPrimaryKey(true)
                .build();
        FieldType fieldType2 = FieldType.newBuilder()
                .withName(MilvusConstants.VECTOR_FIELD_NAME)
                .withDescription("record embedding")
                .withDataType(DataType.FloatVector)
                .withDimension(MilvusConstants.VECTOR_DIMENSION)
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
                    .withFieldName(MilvusConstants.VECTOR_FIELD_NAME)
                    .withIndexName(indexName)
                    .withIndexType(IndexType.IVF_SQ8) // copied from original code by Pangeanic
                    .withMetricType(MilvusConstants.INDEX_METRIC_TYPE) // not sure, using L2 for now
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
        LOG.debug("Listing partitions for collection {}...", collectionName);
        R<ShowPartitionsResponse> partitionsResponse = MilvusUtils.checkResponse(
                milvusClient.showPartitions(ShowPartitionsParam.newBuilder()
                        .withCollectionName(collectionName)
                        .build()), "Error listing partitions");
        List<String> result = new ArrayList<>();
        for (String partition : partitionsResponse.getData().getPartitionNamesList()) {
            LOG.debug(partition);
            result.add(partition);
            if (maxPartitions != null && result.size() >= maxPartitions) {
                LOG.debug("Only listing first {} partitions", maxPartitions);
                break;
            }
        }
        return result;
    }

    /**
     * Retrieve the total number of items present in a particular collection
     * @param milvusClient the client to use
     * @param collectionName the collection to query
     * @return total number of stored entries
     */
    public static long getCount(MilvusClient milvusClient, String collectionName) {
        LOG.debug("Retrieving statistics for collection {}...", collectionName);
        R<GetCollectionStatisticsResponse> statsResponse = MilvusUtils.checkResponse(milvusClient.getCollectionStatistics(GetCollectionStatisticsParam.newBuilder()
                .withCollectionName(collectionName)
                .build()));
        return new GetCollStatResponseWrapper(statsResponse.getData()).getRowCount();
    }

    /**
     * Delete a collection with the provided name, using the provided client
     * @param milvusClient
     * @param collectionName
     */
    public static void deleteCollection(MilvusClient milvusClient, String collectionName) {
        LOG.info("Dropping Milvus collection {}...", collectionName);
        LOG.info(checkResponse(milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .build())));
    }

}
