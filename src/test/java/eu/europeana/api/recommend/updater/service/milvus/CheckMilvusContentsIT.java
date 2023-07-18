package eu.europeana.api.recommend.updater.service.milvus;

import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.*;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.GetCollectionStatisticsParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.collection.ReleaseCollectionParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.highlevel.collection.ListCollectionsParam;
import io.milvus.param.highlevel.collection.response.ListCollectionsResponse;
import io.milvus.param.highlevel.dml.GetIdsParam;
import io.milvus.param.highlevel.dml.response.GetResponse;
import io.milvus.param.index.GetIndexStateParam;
import io.milvus.param.partition.ShowPartitionsParam;
import io.milvus.response.QueryResultsWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * This is a special test class that we use to check what is in a provided Milvus collection.
 * This means that this test is usually disabled and we only start it manually when necessary.
 *
 * @author Patrick Ehlert
 */
@Disabled("Run this manually if you want to check contents of a milvus collection")
public class CheckMilvusContentsIT {

    private static final Logger LOG = LogManager.getLogger(CheckMilvusContentsIT.class);

    private static final String SERVER_URL = "localhost";
    private static final int SERVER_PORT = 19530;
    private static final String TEST_COLLECTION = "test";


    private MilvusClient setup() {
        LOG.info("Setting up connection to Milvus at {}...", SERVER_URL);
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost(SERVER_URL)
                .withPort(SERVER_PORT)
                .build();
        MilvusClient milvusClient = new MilvusServiceClient(connectParam);
        R response = MilvusUtils.checkResponse(milvusClient.checkHealth(), "Error checking client health");
        LOG.info("Milvus connection ok: " + response);
        return milvusClient;
    }

    private boolean testCollectionAvailable(MilvusClient milvusClient) {
        LOG.info("Checking collections...");
        R<ListCollectionsResponse> response = MilvusUtils.checkResponse(milvusClient.listCollections(ListCollectionsParam.newBuilder().build()));
        List<String> collectionNames = response.getData().collectionNames;
        LOG.info("Available collections are: {}", collectionNames);
        return collectionNames.contains(TEST_COLLECTION);
    }

    @Test
    public void testMilvus() {
        MilvusClient milvusClient = setup();

        if (testCollectionAvailable(milvusClient)) {
            LOG.info("Listing partitions for collection {}...", TEST_COLLECTION);
            R<ShowPartitionsResponse> partitionsResponse = MilvusUtils.checkResponse(milvusClient.showPartitions(ShowPartitionsParam.newBuilder()
                    .withCollectionName(TEST_COLLECTION)
                    .build()));
            int p = 0;
            for (String partition : partitionsResponse.getData().getPartitionNamesList()) {
                LOG.info("  {}", partition);
                p++;
                if (p == 100) {
                    LOG.info("Only listing first 100 partitions");
                    break;
                }
            }

            R<GetIndexStateResponse> indexResponse = MilvusUtils.checkResponse(milvusClient.getIndexState(GetIndexStateParam.newBuilder()
                    .withCollectionName(TEST_COLLECTION)
                    .build()));
            LOG.info("Index state: {}", indexResponse.getData().getState());

            R<GetCollectionStatisticsResponse> statsResponse = MilvusUtils.checkResponse(milvusClient.getCollectionStatistics(GetCollectionStatisticsParam.newBuilder()
                    .withCollectionName(TEST_COLLECTION)
                    .build()));
            LOG.info("Retrieved statistics for collection {}:", TEST_COLLECTION);
            for (KeyValuePair keyValue : statsResponse.getData().getStatsList()) {
                LOG.info("  {} = {}", keyValue.getKey(), keyValue.getValue());
            }

            LOG.info("Listing 10 items...");
            loadCollection(milvusClient, TEST_COLLECTION);
            R<QueryResults> results = MilvusUtils.checkResponse(milvusClient.query(QueryParam.newBuilder()
                            .withCollectionName(TEST_COLLECTION)
                            .withOutFields(List.of(MilvusUtils.RECORD_ID_KEY, MilvusUtils.VECTOR_VALUE))
                            .withExpr(MilvusUtils.RECORD_ID_KEY + " like '%'")
                            .withLimit(10L)
                    .build()));
            StringArray ids = results.getData().getFieldsData(0).getScalars().getStringData();
            FloatArray vectors = results.getData().getFieldsData(1).getVectors().getFloatVector();
            // TODO find out if there's an easier way to get keys and values
            //  Right now all vectors are in one big array, so we need to read them back per 300
            int v = 0;
            for (int i = 0; i < 10; i++) {
                StringBuilder vector = new StringBuilder().append(vectors.getData(v)).append(',');
                v++;
                while (v % MilvusUtils.COLLECTION_DIMENSION != 0) {
                    vector.append(vectors.getData(v)).append(',');
                    v++;
                }
                LOG.info("{} = {}", ids.getData(i), vector.toString());
            }
            releaseCollection(milvusClient, TEST_COLLECTION);
        } else {
            LOG.warn("Test collection {} is not available", TEST_COLLECTION);
        }
    }
    private void loadCollection(MilvusClient milvusClient, String colletionName) {
        MilvusUtils.checkResponse(milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(colletionName)
                .build()));
    }
    private void releaseCollection(MilvusClient milvusClient, String collectionName) {
        MilvusUtils.checkResponse(milvusClient.releaseCollection(ReleaseCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .build()));
    }

    /**
     * This test can be used to retrieve the vectors for one or more record ids
     */
    @Test
    public void testGetVectorForID() {
        MilvusClient milvusClient = setup();
        loadCollection(milvusClient, TEST_COLLECTION);

        List<String> ids = List.of("'08607/799647'", "'08607/104637'", "'08607/799845'");
        R<GetResponse> response = MilvusUtils.checkResponse(milvusClient.get(GetIdsParam.newBuilder()
                        .withCollectionName(TEST_COLLECTION)
                        .withPrimaryIds(ids)
                        .build()));
        List<QueryResultsWrapper.RowRecord> vectors = response.getData().getRowRecords();
        for (int i = 0; i < vectors.size(); i++) {
            LOG.info("id = {} -> vectors = {}", ids.get(i), vectors.get(i));
        }

        releaseCollection(milvusClient, TEST_COLLECTION);
    }

//    @Disabled("Only enable this when creating a new collection manually")
//    @Test
//    public void createCollection() {
//        MilvusClient milvusClient = setup();
//        MilvusUtils.createCollection(milvusClient, TEST_COLLECTION, "Test collection", TEST_COLLECTION + INDEX_SUFFIX);
//    }
//
//    @Disabled("Only enable this when deleting a partition manually")
//    @Test
//    public void deletePartition() {
//        MilvusClient milvusClient = setup();
//        String partitionToDelete = "92087";
//        LOG.info(MilvusUtils.checkResponse(milvusClient.dropPartition(DropPartitionParam.newBuilder()
//                        .withCollectionName(TEST_COLLECTION)
//                        .withPartitionName(partitionToDelete)
//                        .build()), "Error removing partition " + partitionToDelete));
//    }
//
//    @Disabled("Only enable this when deleting a new collection manually")
//    @Test
//    public void deleteCollection() {
//        MilvusClient milvusClient = setup();
//        MilvusUtils.deleteCollection(milvusClient, TEST_COLLECTION);
//    }

}
