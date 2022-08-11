package eu.europeana.api.recommend.updater.service.milvus;

import eu.europeana.api.recommend.updater.config.JobCmdLineStarter;
import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import eu.europeana.api.recommend.updater.exception.MilvusStateException;
import eu.europeana.api.recommend.updater.model.embeddings.RecordVectors;
import eu.europeana.api.recommend.updater.util.AverageTime;
import io.milvus.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.*;

/**
 * Service that sets up connection to Milvus and writes vectors to it.
 *
 * The current Milvus version doesn't support string ids, so at the moment we write 2 mappings to a local lmdb database.
 * One is the id2key mapping and the other is the key2id mapping.
 *
 * @author Patrick Ehlert
 */
@Service
@SuppressWarnings("fb-contrib:USFW_UNSYNCHRONIZED_SINGLETON_FIELD_WRITES")
public class MilvusWriterService implements ItemWriter<List<RecordVectors>>, JobExecutionListener {

    private static final Logger LOG = LogManager.getLogger(MilvusWriterService.class);

    private final UpdaterSettings settings;
    private final String collectionName; // used for both lmdb and Milvus!

    private final LmdbWriterService lmdbWriterService;
    private boolean isFullUpdate;
    private MilvusClient milvusClient;
    private AverageTime averageTimeLMDB;  // for debugging purposes
    private AverageTime averageTimeMilvus;  // for debugging purposes

    private Set<String> partitionNames = null; // to keep track which sets (partitions) are present in Milvus

    public MilvusWriterService(UpdaterSettings settings, LmdbWriterService lmdbWriterService) {
        this.settings = settings;
        this.collectionName = settings.getMilvusCollection();
        this.lmdbWriterService = lmdbWriterService;
        if (LOG.isDebugEnabled()) {
            this.averageTimeLMDB = new AverageTime(settings.getLogTimingInterval(), "writing to LMDB");
            this.averageTimeMilvus = new AverageTime(settings.getLogTimingInterval(), "writing to Milvus");
        }
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

            if (settings.useMilvusPartitions()) {
                ListPartitionsResponse response = milvusClient.listPartitions(this.collectionName);
                checkMilvusResponse(response.getResponse(), "Error listing partitions");
                this.partitionNames = new HashSet(response.getPartitionList());
                LOG.info("Found {} partitions", partitionNames.size());
            }
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
    }

    private void createNewCollection(String collectionName) {
        // Method is copied from https://bitbucket.org/jhn-ngo/recsy-xx/src/master/src/engines/searchers/engine.py
        CollectionMapping newCollection = new CollectionMapping.Builder(collectionName, 300).build();
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
    @SuppressWarnings("fb-contrib:USFW_UNSYNCHRONIZED_SINGLETON_FIELD_WRITES")
    private void shutdown() {
        if (lmdbWriterService != null) {
            lmdbWriterService.shutdown();
        }
        if (milvusClient != null) {
            LOG.info("Closing connection to Milvus.");
            milvusClient.close();
            milvusClient = null;
        }
    }

    @Override
    public void write(List<? extends List<RecordVectors>> lists) {

        String setName = null; // only used when writing to partitions
        List<String> recordIds = new ArrayList<>();
        List<List<Float>> vectors = new ArrayList<>();

        long start = System.currentTimeMillis();
        // first gather all recordIds and corresponding vectors
        for (List<RecordVectors> list : lists) {
            for (RecordVectors recvec : list) {
                recordIds.add(recvec.getId());
                vectors.add(Arrays.asList(recvec.getEmbedding()));
            }
        }

        // write recordIds to lmdb (that generates the long ids for milvus)
        List<Long> ids = lmdbWriterService.writeIds(recordIds);

        if (LOG.isDebugEnabled()) {
            long duration = System.currentTimeMillis() - start;
            averageTimeLMDB.addTiming(duration);
            LOG.trace("4a. Saved {} ids to LMDB in {} ms", ids.size(), duration);
        }

        start = System.currentTimeMillis();
        if (!ids.isEmpty()) {
            // determine setname to use as milvus partition name
            if (settings.useMilvusPartitions()) {
                setName = recordIds.get(0).split("/")[0];
                LOG.trace("Setname is {} ", setName);
            }
            writeToMilvus(setName, ids, vectors);
        }
        if (LOG.isDebugEnabled()) {
            long duration = System.currentTimeMillis() - start;
            averageTimeMilvus.addTiming(duration);
            LOG.trace("4b. Saved {} vectors in Milvus partition {} in {} ms", ids.size(), setName, duration);
        }

    }

    private void writeToMilvus(String setName, List<Long> ids, List<List<Float>> vectors) {
        if (setName == null) {
            LOG.trace("Writing {} records to Milvus...", ids.size());
        } else {
            LOG.trace("Writing {} records for set {} to Milvus...", ids.size(), setName);
        }

        InsertParam.Builder insertBuilder = new InsertParam.Builder(collectionName)
                .withVectorIds(ids)
                .withFloatVectors(vectors);

        if (setName != null) {
            // do we need to create a new partition first?
            if (!partitionNames.contains(setName)) {
                LOG.debug("Creating new milvus partition {}", setName);
                checkMilvusResponse(milvusClient.createPartition(collectionName, setName),
                        "Error creating new partition for set " + setName);
                partitionNames.add(setName);
            }
            insertBuilder.withPartitionTag(setName);
        }

        InsertResponse response = milvusClient.insert(insertBuilder.build());
        if (!response.ok()) {
            throw new MilvusStateException("Error writing vectors to Milvus: " + response.getResponse().getMessage() +
                    "/nVectorIds = "+response.getVectorIds());
        }
    }

}
