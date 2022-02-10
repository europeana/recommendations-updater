package eu.europeana.api.recommend.updater.service.milvus;

import eu.europeana.api.recommend.updater.config.JobCmdLineStarter;
import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import eu.europeana.api.recommend.updater.exception.MilvusStateException;
import eu.europeana.api.recommend.updater.model.embeddings.RecordVectors;
import io.milvus.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Service that sets up connection to Milvus and writes vectors to it.
 *
 * The current Milvus version doesn't support string ids, so at the moment we write 2 mappings to a local lmdb database.
 * One is the id2key mapping and the other is the key2id mapping.
 *
 * @author Patrick Ehlert
 */
@Service
public class MilvusWriterService implements ItemWriter<List<RecordVectors>>, JobExecutionListener {

    private static final Logger LOG = LogManager.getLogger(MilvusWriterService.class);

    private final UpdaterSettings settings;
    private final String collectionName; // used for both lmdb and Milvus!

    private final LmdbWriterService lmdbWriterService;
    private boolean isFullUpdate;
    private MilvusClient milvusClient;

    //private Set<String> partitionNames = new HashSet(); // to keep track which sets (partitions) are present in Milvus

    public MilvusWriterService(UpdaterSettings settings, LmdbWriterService lmdbWriterService) {
        this.settings = settings;
        this.collectionName = settings.getMilvusCollection();
        this.lmdbWriterService = lmdbWriterService;
    }

    /**
     * Setup connection to milvus and create new collection if necessary
     * @param jobExecution
     */
    @Override
    @SuppressWarnings("fb-contrib:USFW_UNSYNCHRONIZED_SINGLETON_FIELD_WRITES")
    public void beforeJob(JobExecution jobExecution) {
        this.isFullUpdate = JobCmdLineStarter.isFullUpdate(jobExecution.getJobParameters());
        boolean isDeleteDb = JobCmdLineStarter.isDeleteDb(jobExecution.getJobParameters());

        if (UpdaterSettings.isValueDefined(settings.getMilvusUrl())) {
            long count = lmdbWriterService.init(isFullUpdate, isDeleteDb);

            LOG.info("Setting up connection to Milvus at {}...", settings.getMilvusUrl());
            ConnectParam connectParam = new ConnectParam.Builder()
                    .withHost(settings.getMilvusUrl())
                    .withPort(settings.getMilvusPort())
                    .build();
            milvusClient = new MilvusGrpcClient(connectParam);

            LOG.info("Milvus connection ok. Checking collections...");

            List<String> collections = milvusClient.listCollections().getCollectionNames();
            LOG.info("Available collections are: {}", collections);
            checkMilvusCollectionsState(collections, count, isDeleteDb);
        }
    }

    private void checkMilvusCollectionsState(List<String> collectionNames, long expectedCount, boolean deleteOldData) {
        long nrEntities;
        if (collectionNames.contains(collectionName)) {
            if (deleteOldData) {
                LOG.info("Deleting old collection {}...", collectionName);
                checkMilvusResponse(milvusClient.dropCollection(collectionName),
                        "Error dropping Milvus collection " + collectionName);

                LOG.info("Creating empty new collection named {}...", collectionName);
                createNewCollection(collectionName);
                nrEntities = 0;
            } else {
                nrEntities = milvusClient.countEntities(settings.getMilvusCollection()).getCollectionEntityCount();
            }
        } else {
            LOG.info("Creating empty new collection named {}...", collectionName);
            createNewCollection(collectionName);
            nrEntities = 0;
        }

        // check if count is as expected
        if (nrEntities != expectedCount) {
            LOG.error("Expected {} items in Milvus collection {}, but found {}", expectedCount, collectionName, nrEntities);
            throw new MilvusStateException("Aborting update because of inconsistent state");
        } else {
            LOG.info("Collection {} has {} entities", settings.getMilvusCollection(), nrEntities);
        }

        // for full update check if database is empty
        if (isFullUpdate && nrEntities > 0) {
            LOG.error("Found collection {} containing {} entities", collectionName, nrEntities);
            throw new MilvusStateException("Aborting full update because Milvus target collection exists and is not empty");
        }

        // for partial update check if database is not empty
        if (!isFullUpdate && nrEntities == 0) {
            LOG.warn("Found empty milvus collection. Are you sure you want to do a partial update?");
        }

        //if (settings.useMilvusPartitions()) {
        //    this.partitionNames = new HashSet(milvusClient.listPartitions(collectionName).getPartitionList());
        //}
    }

    private void createNewCollection(String collectionName) {
        // Method is copied from https://bitbucket.org/jhn-ngo/recsy-xx/src/master/src/engines/searchers/engine.py
        CollectionMapping newCollection = new CollectionMapping.Builder(collectionName, 1024).build();
        checkMilvusResponse(milvusClient.createCollection(newCollection),
                "Error creating collection");

        Index newIndex = new Index.Builder(settings.getMilvusCollection(), IndexType.IVF_SQ8)
                .withParamsInJson("{\"nlist\": 16384}").build();
        checkMilvusResponse(milvusClient.createIndex(newIndex), "Error creating index");
    }

    private void checkMilvusResponse(Response response, String errorMsg) {
        if (!response.ok()) {
            throw new MilvusStateException(errorMsg + ": " + response.getMessage());
        }
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        this.shutdown();
    }

    @PreDestroy
    private void shutdown() {
        lmdbWriterService.shutdown();
        if (milvusClient != null) {
            LOG.info("Closing connection to Milvus.");
            milvusClient.close();
            milvusClient = null;
        }
    }

    @Override
    public void write(List<? extends List<RecordVectors>> lists) {
        long start = System.currentTimeMillis();
        String setName = null;
        List<Long> ids = new ArrayList<>();
        List<List<Float>> vectors = new ArrayList<>();

        for (List<RecordVectors> list : lists) {
            for (RecordVectors recvec : list) {

                long newId = lmdbWriterService.writeId(recvec.getId());
//                String recordSetName = EuropeanaIdUtils.getSetName(recvec.getId());
//                if (settings.useMilvusPartitions() && setName == null) {
//                    setName = recordSetName;
//                }
//
//                if (!setName.equalsIgnoreCase(recordSetName) && !ids.isEmpty()) {
//                    // we're starting a new set, write previous set.
//                    writeToMilvus(setName, ids, vectors);
//                    LOG.debug("Finished writing set {}, starting set{}", setName, recordSetName);
//                    setName = recordSetName;
//                    ids = new ArrayList<>();
//                    vectors = new ArrayList<>();
//                }

                ids.add(newId);
                vectors.add(Arrays.asList(recvec.getEmbedding()));
            }
        }

        if (!ids.isEmpty()) {
            writeToMilvus(setName, ids, vectors);
        }
        LOG.debug("4. Saved {} vectors in Milvus in {} ms", ids.size(), System.currentTimeMillis() - start);

    }

    private void writeToMilvus(String setName, List<Long> ids, List<List<Float>> vectors) {
        if (setName == null) {
            LOG.trace("Writing {} records to Milvus...", ids.size());
        } else {
            LOG.trace("Writing {} records for set {} to Milvus...", ids.size(), setName);
        }

//        if (setName != null && !partitionNames.contains(setName)) {
//            checkMilvusResponse(milvusClient.createPartition(collectionName, setName),
//                    "Error creating new partition for set " + setName);
//            partitionNames.add(setName);
//        }

        InsertParam insrt = new InsertParam.Builder(collectionName)
                .withVectorIds(ids)
                .withFloatVectors(vectors)
            //    .withPartitionTag(setName)
                .build();
        InsertResponse response = milvusClient.insert(insrt);
        if (!response.ok()) {
            throw new MilvusStateException("Error writing vectors to Milvus: " + response.getResponse().getMessage() +
                    "/nVectorIds = "+response.getVectorIds());
        }
    }

}
