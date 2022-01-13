package eu.europeana.api.recommend.updater.service.milvus;

import eu.europeana.api.commons.error.EuropeanaApiException;
import eu.europeana.api.recommend.updater.config.JobCmdLineStarter;
import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import eu.europeana.api.recommend.updater.exception.MilvusStateException;
import eu.europeana.api.recommend.updater.model.embeddings.RecordVectors;
import io.milvus.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Service that sets up connection to Milvus and writes vectors to it
 *
 * @author Patrick Ehlert
 */
@Service
public class MilvusWriterService implements JobExecutionListener, ItemWriter<List<RecordVectors>> {

    private static final Logger LOG = LogManager.getLogger(MilvusWriterService.class);
    private static final Object lock = new Object();

    private final UpdaterSettings settings;
    private final String collectionName;

    private boolean isFullUpdate;
    private MilvusClient milvusClient;
    private Set<String> partitionNames = new HashSet(); // to keep track which sets (partitions) are present in Milvus

    private Long counter = 0L; // tmp so we can test writing data to Milvus

    public MilvusWriterService(UpdaterSettings settings) {
        this.settings = settings;
        this.collectionName = settings.getMilvusCollection();
    }

    /**
     * Setup connection to milvus and create new collection if necessary
     * @param jobExecution
     */
    @Override
    public void beforeJob(JobExecution jobExecution) {
        this.isFullUpdate = JobCmdLineStarter.isFullUpdate(jobExecution.getJobParameters());

        if (UpdaterSettings.isValueDefined(settings.getMilvusUrl())) {
            LOG.info("Setting up connection to Milvus at {}...", settings.getMilvusUrl());
            ConnectParam connectParam = new ConnectParam.Builder()
                    .withHost(settings.getMilvusUrl())
                    .withPort(settings.getMilvusPort())
                    .build();
            milvusClient = new MilvusGrpcClient(connectParam);

            LOG.info("Milvus connection ok. Checking collections...");
            try {
                checkMilvusCollectionsState(milvusClient.listCollections().getCollectionNames());
            } catch (EuropeanaApiException e) {
                LOG.error("Error checking Milvus state", e);
                jobExecution.setExitStatus(ExitStatus.FAILED);
            }
        }
    }

    private void checkMilvusCollectionsState(List<String> collectionNames) throws EuropeanaApiException {
        // check if configured collection is available and empty or create new
        if (collectionNames.contains(collectionName)) {
            long size = milvusClient.countEntities(settings.getMilvusCollection()).getCollectionEntityCount();
            if (isFullUpdate && size > 0) {
                LOG.error("Found collection {} containing {} entities", collectionName, size);
                throw new MilvusStateException("Aborting full-update because target collection exists and is not empty");
            }
            if (!isFullUpdate && size == 0) {
                LOG.warn("Found empty collection while doing partial update. Are you sure you don't want to do a full update?");
            }

            if (settings.useMilvusPartitions()) {
                this.partitionNames = new HashSet(milvusClient.listPartitions(collectionName).getPartitionList());
            }
        } else {
            LOG.info("Creating empty new collection named {}...", collectionName);

            // Create a Milvus collection and index
            // Settings are copied from https://bitbucket.org/jhn-ngo/recsy-xx/src/master/src/engines/searchers/engine.py
            CollectionMapping newCollection =  new CollectionMapping.Builder(collectionName, 1024).build();
            checkMilvusResponse(milvusClient.createCollection(newCollection),
                "Error creating collection");

            Index newIndex = new Index.Builder(settings.getMilvusCollection(), IndexType.IVF_SQ8)
                    .withParamsInJson("{\"nlist\": 16384}").build();
            checkMilvusResponse(milvusClient.createIndex(newIndex),
                    "Error creating index");
        }
    }

    private void checkMilvusResponse(Response response, String errorMsg) throws EuropeanaApiException {
        if (!response.ok()) {
            throw new MilvusStateException(errorMsg + ": " + response.getMessage());
        }
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (milvusClient != null) {
            LOG.info("Closing connection to Milvus.");
            milvusClient.close();
        }
    }

    @Override
    public void write(List<? extends List<RecordVectors>> lists) throws EuropeanaApiException {
        String setName = null;
        List<Long> ids = new ArrayList<>();
        List<List<Float>> vectors = new ArrayList<>();

        for (List<RecordVectors> list : lists) {
            for (RecordVectors recvec : list) {

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

                // TODO save record to lmdb
                synchronized (lock) {
                    counter++;
                }
                ids.add(counter);
                vectors.add(Arrays.asList(recvec.getEmbedding()));
            }
        }

        if (!ids.isEmpty()) {
            writeToMilvus(setName, ids, vectors);
        }
    }

    private void writeToMilvus(String setName, List<Long> ids, List<List<Float>> vectors) throws EuropeanaApiException {
        if (setName == null) {
            LOG.debug("Writing {} records to Milvus...", ids.size());
        } else {
            LOG.debug("Writing {} records for set {} to Milvus...", ids.size(), setName);
        }

        if (!partitionNames.contains(setName)) {
            checkMilvusResponse(milvusClient.createPartition(collectionName, setName),
                    "Error creating new partition for set " + setName);
            partitionNames.add(setName);
        }

        InsertParam insrt = new InsertParam.Builder(collectionName)
                .withVectorIds(ids)
                .withFloatVectors(vectors)
                .withPartitionTag(setName)
                .build();
        InsertResponse response = milvusClient.insert(insrt);
        if (!response.ok()) {
            throw new MilvusStateException("Error writing vectors to Milvus: " + response.getResponse().getMessage() +
                    "/nVectorIds = "+response.getVectorIds());
        }
    }

}
