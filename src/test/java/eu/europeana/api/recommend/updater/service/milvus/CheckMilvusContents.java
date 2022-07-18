package eu.europeana.api.recommend.updater.service.milvus;

import io.milvus.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This is a special test class that we use to check what is in a provided Milvus collection.
 * This means that this test is usually disabled and we only start it manually when necessary.
 *
 * @author Patrick Ehlert
 */
@Disabled("Run this manually if you want to check contents of a milvus collection")
public class CheckMilvusContents {

    private static final Logger LOG = LogManager.getLogger(CheckMilvusContents.class);

    private static final String SERVER_URL = "";
    private static final int SERVER_PORT = 0;
    private static final String COLLECTION = "test";


    private MilvusClient setup() {
        LOG.info("Setting up connection to Milvus at {}...", SERVER_URL);
        ConnectParam connectParam = new ConnectParam.Builder()
                .withHost(SERVER_URL)
                .withPort(SERVER_PORT)
                .build();
        MilvusClient milvusClient = new MilvusGrpcClient(connectParam);
        LOG.info("Milvus connection ok. Checking collections...");

        List<String> collections = milvusClient.listCollections().getCollectionNames();
        LOG.info("Available collections are: {}", collections);
        assertTrue(collections.contains(COLLECTION));
        return milvusClient;
    }

    @Test
    public void testMilvus() {
        MilvusClient milvusClient = setup();

        LOG.info("Listing partitions for collection {}...", COLLECTION);
        ListPartitionsResponse partitionsResponse = milvusClient.listPartitions(COLLECTION);
        assertTrue(partitionsResponse.ok());
        int i = 0;
        for (String partition : partitionsResponse.getPartitionList()) {
            LOG.info(partition);
            i++;
            if (i == 100) {
                LOG.info("Only listing first 100 partitions");
                break;
            }
        }

        long count = milvusClient.countEntities(COLLECTION).getCollectionEntityCount();
        LOG.info("Found {} entries in collection {}", count, COLLECTION);

        LOG.info("Listing first 10 items...");
        List<Long> ids = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        GetEntityByIDResponse response = milvusClient.getEntityByID(COLLECTION, ids);
        assertTrue(response.ok());
        for (int j = 0; j < 10; j++) {
            LOG.info("{} = {}", j, response.getFloatVectors().get(j));
        }
    }

    /**
     * This test can be used to retrieve the vectors for one or more specific LMDB ids
     */
    @Test
    public void testGetVectorForID() {
        MilvusClient milvusClient = setup();
        List<Long> lmdbIds = List.of(29086550L, 28764443L, 9913235L, 10224472L);
        GetEntityByIDResponse response = milvusClient.getEntityByID(COLLECTION, lmdbIds);
        assertTrue(response.ok());

        for (int i = 0; i < lmdbIds.size(); i++) {
            LOG.info("LmdbId = {} -> Vectors = {}", lmdbIds.get(i), response.getFloatVectors().get(i));
        }
    }

}
