package eu.europeana.api.recommend.updater.service.milvus;

import eu.europeana.api.recommend.common.MilvusConstants;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.*;
import io.milvus.param.ConnectParam;
import io.milvus.param.IndexType;
import io.milvus.param.R;
import io.milvus.param.collection.*;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.highlevel.collection.ListCollectionsParam;
import io.milvus.param.highlevel.collection.response.ListCollectionsResponse;
import io.milvus.param.highlevel.dml.GetIdsParam;
import io.milvus.param.highlevel.dml.response.GetResponse;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.GetIndexStateParam;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.response.SearchResultsWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Date;
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
        LOG.info("Setting up connection to Milvus at {}:{}...", SERVER_URL, SERVER_PORT);
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
        R<ListCollectionsResponse> responseList = MilvusUtils.checkResponse(milvusClient.listCollections(ListCollectionsParam.newBuilder().build()));
        List<String> collectionNames = responseList.getData().collectionNames;
        LOG.info("Available collections are: {}", collectionNames);
        boolean result = collectionNames.contains(TEST_COLLECTION);
        if (result) {
            R<DescribeCollectionResponse> responseDescribe = MilvusUtils.checkResponse(milvusClient.describeCollection(DescribeCollectionParam.newBuilder()
                            .withCollectionName(TEST_COLLECTION)
                    .build()));
            DescribeCollectionResponse r = responseDescribe.getData();
            // TODO is there a method to retrieve the collection's description!?
            LOG.info("Collection {} was created on {}", r.getCollectionName(), new Date(r.getCreatedUtcTimestamp()));
        }
        return result;
    }

    @Test
    public void testMilvus() {
        MilvusClient milvusClient = setup();

        if (testCollectionAvailable(milvusClient)) {

            List<String> partitions = MilvusUtils.getPartitions(milvusClient, TEST_COLLECTION);
            LOG.info("Found partitions: {}", partitions);

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

            try {
                R<GetLoadingProgressResponse> loadingResponse = MilvusUtils.checkResponse(milvusClient.getLoadingProgress(GetLoadingProgressParam.newBuilder()
                        .withCollectionName(TEST_COLLECTION)
                        .build()));
                LOG.info("Loading progress is {}%, error status = {}", loadingResponse.getData().getProgress(),
                        loadingResponse.getData().getStatus());
            } catch (RuntimeException e) {
                // Note that this will be triggered also when the connection is not loaded
                LOG.error("Error checking if collection is loaded {}", e.getMessage());
            }

            LOG.info("Loading collection...");
            loadCollection(milvusClient, TEST_COLLECTION);

            LOG.info("Listing 10 items...");
            Long start = System.currentTimeMillis();
            R<QueryResults> results = MilvusUtils.checkResponse(milvusClient.query(QueryParam.newBuilder()
                            .withCollectionName(TEST_COLLECTION)
                            .withOutFields(List.of(MilvusConstants.RECORD_ID_FIELD_NAME, MilvusConstants.VECTOR_FIELD_NAME))
                            .withExpr(MilvusConstants.RECORD_ID_FIELD_NAME + " like '%'")
                            .withLimit(10L)
                    .build()));
            StringArray ids = results.getData().getFieldsData(0).getScalars().getStringData();
            FloatArray vectors = results.getData().getFieldsData(1).getVectors().getFloatVector();
            // TODO find out if there's an easier way to get keys and values
            //  Right now all vectors are in one big array, so we need to read them back per 300
            if (vectors.getDataCount() == 0) {
                LOG.info("No vectors found to list");
            }
            int v = 0;
            for (int i = 0; i < Math.min(10, vectors.getDataCount()); i++) {
                StringBuilder vector = new StringBuilder().append(vectors.getData(v)).append(',');
                v++;
                while (v % MilvusConstants.VECTOR_DIMENSION != 0) {
                    vector.append(vectors.getData(v)).append(',');
                    v++;
                }
                LOG.info("{} = {}", ids.getData(i), vector.toString());
            }
            LOG.info("Done listing in {} ms", System.currentTimeMillis() - start);

            LOG.info("Releasing collection...");
            releaseCollection(milvusClient, TEST_COLLECTION);
        } else {
            LOG.warn("Test collection {} is not available", TEST_COLLECTION);
        }
        milvusClient.close();
    }
    private void loadCollection(MilvusClient milvusClient, String collectionName) {
        MilvusUtils.checkResponse(milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .build()));
    }
    private void releaseCollection(MilvusClient milvusClient, String collectionName) {
        LOG.warn("Releasing collection disabled because loading it again takes a long time with a full database");
//        MilvusUtils.checkResponse(milvusClient.releaseCollection(ReleaseCollectionParam.newBuilder()
//                .withCollectionName(collectionName)
//                .build()));
    }

    /**
     * This test can be used to retrieve the vectors for one or more record ids
     */
    @Test
    public void testGetVectorForID() {
        MilvusClient milvusClient = setup();
        loadCollection(milvusClient, TEST_COLLECTION);

        List<String> ids = List.of("08607/799647", "08607/104637", "08607/799845");
        R<GetResponse> response = MilvusUtils.checkResponse(milvusClient.get(GetIdsParam.newBuilder()
                        .withCollectionName(TEST_COLLECTION)
                        .withPrimaryIds(ids)
                        .build()));
        List<QueryResultsWrapper.RowRecord> vectors = response.getData().getRowRecords();
        for (int i = 0; i < vectors.size(); i++) {
            LOG.info("id = {} -> vectors = {}", ids.get(i), vectors.get(i));
        }

        LOG.info("Releasing collection...");
        releaseCollection(milvusClient, TEST_COLLECTION);
        milvusClient.close();
    }

    /**
     * Given a particular record ID, find the 3 records that are most similar (see also https://milvus.io/docs/search.md)
     */
    @Test
    public void testGetSimilarRecords() {
        MilvusClient milvusClient = setup();
        LOG.info("Loading collection...");
        loadCollection(milvusClient, TEST_COLLECTION);
        String recordToSearch = "2020702/raa_fmi_10000100010001";

        // get vector of record to compare
        Long start = System.currentTimeMillis();
        R<GetResponse> response = MilvusUtils.checkResponse(milvusClient.get(GetIdsParam.newBuilder()
                .withCollectionName(TEST_COLLECTION)
                .withPrimaryIds(List.of(recordToSearch))
                .build()));
        if (response.getData().getRowRecords().isEmpty()) {
            LOG.error("No vector found for record {} in {} ms", recordToSearch, System.currentTimeMillis() - start);
        } else {
            QueryResultsWrapper.RowRecord result = response.getData().getRowRecords().get(0);
            List<Float> vectors = (List<Float>) result.get(MilvusConstants.VECTOR_FIELD_NAME);

            SearchParam searchParam = SearchParam.newBuilder()
                    .withCollectionName(TEST_COLLECTION)
                    .withMetricType(MilvusConstants.INDEX_METRIC_TYPE) // has to match type in index
                    .withOutFields(List.of(MilvusConstants.RECORD_ID_FIELD_NAME))
                    .withTopK(10) // max 10 results
                    .withVectors(List.of(vectors))
                    .withVectorFieldName(MilvusConstants.VECTOR_FIELD_NAME)
                    .withExpr(MilvusConstants.RECORD_ID_FIELD_NAME + " != '" + recordToSearch +"'") // exclude the record itself
                    //.withParams(SEARCH_PARAM)
                    .build();
            SearchResultsWrapper data = new SearchResultsWrapper(milvusClient.search(searchParam).getData().getResults());

            LOG.info("Retrieved {} items similar to record {} in {} ms", data.getRowRecords(0).size(), recordToSearch, System.currentTimeMillis() - start);
            for (int i = 0; i < data.getRowRecords(0).size(); i++) {
                QueryResultsWrapper.RowRecord record = data.getRowRecords(0).get(i);
                LOG.info("  {} has score {}", record.get(MilvusConstants.RECORD_ID_FIELD_NAME), record.get(MilvusConstants.MILVUS_SCORE_FIELD_NAME));
            }
        }

        LOG.info("Releasing collection...");
        releaseCollection(milvusClient, TEST_COLLECTION);
        milvusClient.close();
    }

    @Disabled ("Only enable this when you need to create a new index after restoring it")
    @Test
    public void createNewindex() {
        String collectionName = TEST_COLLECTION;
        String indexName = collectionName + MilvusWriterService.INDEX_SUFFIX;
        MilvusClient milvusClient = setup();

        LOG.info("Creating index for collection {}...", collectionName);
        LOG.info(MilvusUtils.checkResponse(milvusClient.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(collectionName)
                .withFieldName(MilvusConstants.VECTOR_FIELD_NAME)
                .withIndexName(indexName)
                .withIndexType(IndexType.IVF_SQ8) // copied from original code by Pangeanic
                .withMetricType(MilvusConstants.INDEX_METRIC_TYPE) // not sure, using L2 for now
                .withExtraParam("{\"nlist\": 16384}") // copied from original code by Pangeanic
                //.withSyncMode(Boolean.TRUE) // not sure, not setting for now
                .build()), "Error creating index" + indexName));
    }

//    @Disabled("Only enable this when creating a new collection manually")
//    @Test
//    public void createCollection() {
//        MilvusClient milvusClient = setup();
//        MilvusUtils.createCollection(milvusClient, TEST_COLLECTION, "Test collection", TEST_COLLECTION + INDEX_SUFFIX);
//        milvusClient.close();
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
//        milvusClient.close();
//    }
//
//    @Disabled("Only enable this when deleting a new collection manually")
//    @Test
//    public void deleteCollection() {
//        MilvusClient milvusClient = setup();
//        MilvusUtils.deleteCollection(milvusClient, TEST_COLLECTION);
//        milvusClient.close();
//    }

}
