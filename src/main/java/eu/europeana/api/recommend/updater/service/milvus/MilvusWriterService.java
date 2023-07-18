package eu.europeana.api.recommend.updater.service.milvus;

import eu.europeana.api.recommend.updater.config.JobCmdLineStarter;
import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import eu.europeana.api.recommend.updater.exception.MilvusStateException;
import eu.europeana.api.recommend.updater.model.embeddings.RecordVectors;
import eu.europeana.api.recommend.updater.util.AverageTime;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.FlushParam;
import io.milvus.param.collection.GetCollectionStatisticsParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.highlevel.collection.ListCollectionsParam;
import io.milvus.param.highlevel.collection.response.ListCollectionsResponse;
import io.milvus.param.partition.CreatePartitionParam;
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
 * The current used Milvus version (v2.2.11) supports varchar ids as primary keys
 *
 * @author Patrick Ehlert
 */
@Service
@SuppressWarnings("fb-contrib:USFW_UNSYNCHRONIZED_SINGLETON_FIELD_WRITES")
public class MilvusWriterService implements ItemWriter<List<RecordVectors>>, JobExecutionListener {

    public static final String INDEX_SUFFIX = "Index";

    private static final Logger LOG = LogManager.getLogger(MilvusWriterService.class);

    private final UpdaterSettings settings;
    private final String collectionName;
    private final String collectionDescription;

    private boolean isFullUpdate;
    private MilvusClient milvusClient;
    private Set<String> partitionsExist = new HashSet<>(); // to keep track which sets (partitions) are present in Milvus collection
    private AverageTime averageTimeMilvus;  // for debugging purposes


    public MilvusWriterService(UpdaterSettings settings) {
        this.settings = settings;
        this.collectionName = settings.getMilvusCollection();
        this.collectionDescription = settings.getMilvusCollectionDescription();
        if (LOG.isDebugEnabled()) {
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
            LOG.info("Setting up connection to Milvus at {}...", settings.getMilvusUrl());
            ConnectParam connectParam = ConnectParam.newBuilder()
                    .withHost(settings.getMilvusUrl())
                    .withPort(settings.getMilvusPort())
                    .build();
            milvusClient = new MilvusServiceClient(connectParam);

            LOG.info("Milvus connection ok. Checking collections...");
            R<ListCollectionsResponse> collectionsResponse = milvusClient.listCollections(ListCollectionsParam.newBuilder().build());
            List<String> collectionNames = collectionsResponse.getData().collectionNames;
            LOG.info("Available collections are: {}", collectionNames);

            checkMilvusCollectionsState(collectionNames, isDeleteDb);
        }
    }

    private void checkMilvusCollectionsState(List<String> collectionNames, boolean deleteOldData) {
        long nrEntities;
        if (collectionNames.contains(collectionName)) {
            if (deleteOldData) {
                LOG.info("Deleting old collection {}...", collectionName);
                MilvusUtils.deleteCollection(milvusClient, collectionName);

                LOG.info("Creating empty new collection named {}...", collectionName);
                MilvusUtils.createCollection(milvusClient, collectionName, collectionDescription, collectionName + INDEX_SUFFIX);
                nrEntities = 0;
            } else {
                R<GetCollectionStatisticsResponse> statsResponse = MilvusUtils.checkResponse(
                        milvusClient.getCollectionStatistics(GetCollectionStatisticsParam.newBuilder()
                                .withCollectionName(collectionName)
                                .build()), "Error reading collection statistics");
                nrEntities = statsResponse.getData().getStatsCount();
                LOG.info("Found collection {} containing {} entries", collectionName, nrEntities);

                if (settings.useMilvusPartitions()) {
                    this.partitionsExist = new HashSet<>(MilvusUtils.getPartitions(milvusClient, collectionName));
                    LOG.info("Found {} partitions", partitionsExist.size());
                }
            }
        } else {
            LOG.info("Creating empty new collection named {}...", collectionName);
            MilvusUtils.createCollection(milvusClient, collectionName, collectionDescription, collectionName + INDEX_SUFFIX);
            nrEntities = 0;
        }

        // for full update check if database is empty
        if (isFullUpdate && nrEntities > 0) {
            throw new MilvusStateException("Aborting full update because Milvus target collection exists and is not empty", null);
        }

        // for partial update check if database is not empty
        if (!isFullUpdate && nrEntities == 0) {
            LOG.warn("Empty milvus collection. Are you sure you want to do a partial update?");
        }
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        this.shutdown();
    }

    @PreDestroy
    @SuppressWarnings("fb-contrib:USFW_UNSYNCHRONIZED_SINGLETON_FIELD_WRITES")
    private void shutdown() {
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

        // TODO check if loading a collection on startup increases performance also for writing -> https://milvus.io/docs/load_collection.md
        // first gather all recordIds and corresponding vectors
        for (List<RecordVectors> list : lists) {
            for (RecordVectors recvec : list) {
                recordIds.add(recvec.getId());
                vectors.add(Arrays.asList(recvec.getEmbedding()));
            }
        }

        // Extra check to be sure we have content to write
        if (recordIds.isEmpty()) {
            LOG.debug("No records to write to Milvus");
        } else {
            long start = System.currentTimeMillis();

            // determine setname to use as milvus partition name
            if (settings.useMilvusPartitions()) {
                setName = recordIds.get(0).split("/")[0];
                LOG.trace("Set name is {} ", setName);
            }
            writeToMilvus(setName, recordIds, vectors);

            if (LOG.isDebugEnabled()) {
                long duration = System.currentTimeMillis() - start;
                averageTimeMilvus.addTiming(duration);
                LOG.trace("4. Saved {} vectors in Milvus partition {} in {} ms", recordIds.size(), setName, duration);
            }
        }
    }

    private void writeToMilvus(String setName, List<String> ids, List<List<Float>> vectors) {
        if (setName == null) {
            LOG.trace("Writing {} records to Milvus...", ids.size());
        } else {
            LOG.trace("Writing {} records for set {} to Milvus...", ids.size(), setName);
        }

        List<InsertParam.Field> fields = new ArrayList<>();
        fields.add(new InsertParam.Field(MilvusUtils.RECORD_ID_KEY, ids));
        fields.add(new InsertParam.Field(MilvusUtils.VECTOR_VALUE, vectors));

        InsertParam.Builder insertBuilder = InsertParam.newBuilder()
                .withCollectionName(collectionName)
                .withFields(fields);
        if (settings.useMilvusPartitions() && setName != null) {
            // Keep in mind that there is a 4096 partition limit (see also https://milvus.io/docs/create_collection.md)

            // Do we need to create a new partition first?
            if (!partitionsExist.contains(setName)) {
                LOG.debug("Creating new milvus partition {}", setName);
                MilvusUtils.checkResponse(milvusClient.createPartition(CreatePartitionParam.newBuilder()
                        .withCollectionName(collectionName)
                        .withPartitionName(setName)
                        .build()), "Error creating new partition for set " + setName);
                partitionsExist.add(setName);
            }

            insertBuilder.withPartitionName(setName);
        }
        MilvusUtils.checkResponse(milvusClient.insert(insertBuilder.build()), "Error writing data");

        // Data seems to be written to the Milvus wal file regardless of calling flush or not, but we call it anyway
        // just to be sure.
        MilvusUtils.checkResponse(milvusClient.flush(FlushParam.newBuilder().addCollectionName(collectionName).build()),
                "Error flushing data to Milvus collection " + collectionName);
    }

}
