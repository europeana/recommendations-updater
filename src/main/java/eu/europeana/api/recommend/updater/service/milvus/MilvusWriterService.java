package eu.europeana.api.recommend.updater.service.milvus;

import eu.europeana.api.commons.error.EuropeanaApiException;
import eu.europeana.api.recommend.updater.config.JobCmdLineStarter;
import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import eu.europeana.api.recommend.updater.exception.MilvusStateException;
import eu.europeana.api.recommend.updater.model.embeddings.RecordVectors;
import io.milvus.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lmdbjava.*;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
public class MilvusWriterService implements JobExecutionListener, ItemWriter<List<RecordVectors>> {

    private static final Logger LOG = LogManager.getLogger(MilvusWriterService.class);

    // It seems there is no option to specify a database name and this name is always used;
    private static final String LMDB_FILE_NAME = "data.mdb";
    // Value copied from https://bitbucket.org/jhn-ngo/recsy-xx/src/master/src/recommenders/europeana/indexers/milvus_indexer.py#lines-97
    private static final long LMDB_MAX_SIZE = 50_000_000_000L;
    // Value copied from https://bitbucket.org/jhn-ngo/recsy-xx/src/master/src/recommenders/europeana/indexers/milvus_indexer.py#lines-126
    // It seems this is set because by default keys are limited to 511 bytes (see also https://lmdb.readthedocs.io/en/release/#storage-efficiency-limits)
    private static final int LMDB_MAX_KEY_SIZE = 510;


    private final UpdaterSettings settings;
    private final String collectionName; // used for both lmdb and Milvus!

    private boolean isFullUpdate;
    private MilvusClient milvusClient;

    private Env<ByteBuffer> lmdbEnvironment;
    private Dbi<ByteBuffer> lmdbId2Key; // handle to database
    private Dbi<ByteBuffer> lmdbKey2Id; // handle to 2nd database
    private final Object idLock = new Object(); // lock to prevent concurrency issues
    private Long id = -1L; // saved as recordId number to lmdb,=

    private Set<String> partitionNames = new HashSet(); // to keep track which sets (partitions) are present in Milvus

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
                List<String> collections = milvusClient.listCollections().getCollectionNames();
                LOG.info("Available collections are: {}", collections);
                checkMilvusCollectionsState(collections);
            } catch (EuropeanaApiException e) {
                LOG.error("Error checking Milvus state", e);
                jobExecution.setExitStatus(ExitStatus.FAILED);
            }

            LOG.info("Checking local lmdb database at folder {}", settings.getLmdbFolder());
            setupLmdb(settings.getLmdbFolder(), settings.getMilvusCollection());
        }
    }

    private void checkMilvusCollectionsState(List<String> collectionNames) throws EuropeanaApiException {
        // check if configured collection is available and empty or create new
        if (collectionNames.contains(collectionName)) {
            long size = milvusClient.countEntities(settings.getMilvusCollection()).getCollectionEntityCount();
            LOG.info("Collection {} has {} entities", settings.getMilvusCollection(), size);
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

    private void setupLmdb(String folder,  String tableName) {
        File lmdbFile = new File(folder, LMDB_FILE_NAME);
        if (lmdbFile.exists()) {
            LOG.info("Found existing lmdb database file");
        } else {
            LOG.info("Creating new lmdb database file");
        }

        lmdbEnvironment = Env.create()
                .setMapSize(LMDB_MAX_SIZE)
                .setMaxDbs(2)
                .open(new File(folder));
        lmdbId2Key = lmdbEnvironment.openDbi(tableName + "_id2key", DbiFlags.MDB_CREATE);
        lmdbKey2Id = lmdbEnvironment.openDbi(tableName + "_key2id", DbiFlags.MDB_CREATE);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        this.shutdown();
    }

    @PreDestroy
    private void shutdown() {
        if (lmdbEnvironment != null && ! lmdbEnvironment.isClosed()) {
            LOG.info("Closing connection to local lmdb database");
            lmdbEnvironment.close();
        }
        if (milvusClient != null) {
            LOG.info("Closing connection to Milvus.");
            milvusClient.close();
            milvusClient = null;
        }
    }

    @Override
    public void write(List<? extends List<RecordVectors>> lists) throws EuropeanaApiException {
        Long start = System.currentTimeMillis();
        String setName = null;
        List<Long> ids = new ArrayList<>();
        List<List<Float>> vectors = new ArrayList<>();

        for (List<RecordVectors> list : lists) {
            for (RecordVectors recvec : list) {

                Long newId = writeIdToLmDb(recvec.getId());
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

    private long writeIdToLmDb(String key) {
        // Note that in https://bitbucket.org/jhn-ngo/recsy-xx/src/master/src/recommenders/europeana/indexers/milvus_indexer.py#lines-126
        // keys are encoded using the default Python encode() method (meaning UTF-8) and
        Long newId;
        synchronized (idLock) {
            id++;
            newId = id;
        }

        // For writing to lmdb we followed this tutorial
        // https://github.com/lmdbjava/lmdbjava/blob/master/src/test/java/org/lmdbjava/TutorialTest.java
        ByteBuffer keyValue = ByteBuffer.allocateDirect(LMDB_MAX_KEY_SIZE);
        keyValue.put(Arrays.copyOfRange(key.getBytes(StandardCharsets.UTF_8), 0, LMDB_MAX_KEY_SIZE)).flip();
        ByteBuffer idValue = ByteBuffer.allocateDirect(Long.BYTES).putLong(newId).flip();

        try (Txn<ByteBuffer> transaction = lmdbEnvironment.txnWrite()) {
            // TODO despite setting NODUPDATA and NOOVERWRITE we can reuse an existing database file!?!
            lmdbKey2Id.put(transaction, keyValue, idValue, PutFlags.MDB_NODUPDATA, PutFlags.MDB_NOOVERWRITE);
            lmdbId2Key.put(transaction, idValue, keyValue, PutFlags.MDB_NODUPDATA, PutFlags.MDB_NOOVERWRITE);
        }

        return newId;
    }

    private void writeToMilvus(String setName, List<Long> ids, List<List<Float>> vectors) throws EuropeanaApiException {
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
