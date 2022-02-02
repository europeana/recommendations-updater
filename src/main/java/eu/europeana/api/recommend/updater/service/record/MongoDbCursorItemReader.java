package eu.europeana.api.recommend.updater.service.record;

import eu.europeana.api.recommend.updater.config.JobCmdLineStarter;
import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import eu.europeana.api.recommend.updater.model.record.Record;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Spring Batch reader for reading CHO records from a Mongo database
 * We let each thread download one set, so we don't have to worry about concurrency
 *
 * @author Patrick Ehlert
 */
@Service
public class MongoDbCursorItemReader extends AbstractItemCountingItemStreamItemReader<List<Record>> implements JobExecutionListener {

    private static final Logger LOG = LogManager.getLogger(MongoDbCursorItemReader.class);

    private static final String TOTAL_ITEMS = "TotalItems";

    private final UpdaterSettings settings;
    private final MongoRecordRepository mongoRecordRepository;

    private Stream<Record> stream;
    private Iterator<Record> iterator;
    private Boolean isFullUpdate;
    private Date fromDate;
    private Integer nrRecordsToProcess;

    public MongoDbCursorItemReader(UpdaterSettings settings, MongoRecordRepository mongoRecordRepository, SolrSetReader solrService) {
        this.settings = settings;
        this.mongoRecordRepository = mongoRecordRepository;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        this.setName("Mongo record reader");

        // check job parameters
        isFullUpdate = JobCmdLineStarter.isFullUpdate(jobExecution.getJobParameters());
        fromDate = JobCmdLineStarter.getFromDate(jobExecution.getJobParameters());

        // get list of sets to download from Solr
        // TODO!

        // get total record count from Mongo
        if (this.isFullUpdate) {
            this.nrRecordsToProcess = mongoRecordRepository.countAllBy();
        } else {
            this.nrRecordsToProcess = mongoRecordRepository.countAllByTimestampUpdatedAfter(this.fromDate);
            LOG.info("Total number of records to download {}", nrRecordsToProcess);
        }
        jobExecution.getExecutionContext().put(TOTAL_ITEMS, nrRecordsToProcess);
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        // do nothing
    }

    @Override
    protected void doOpen() {
        LOG.info("{} records to be processed", nrRecordsToProcess);

        if (this.isFullUpdate) {
            this.stream = mongoRecordRepository.streamAllBy();
        } else {
            this.stream = mongoRecordRepository.streamByTimestampUpdatedAfter(this.fromDate);
        }
        this.iterator = stream.iterator();
        LOG.debug("Opened stream to MongoDb");
    }

    @PreDestroy
    @Override
    protected void doClose() {
        if (stream != null) {
            LOG.info("Closing mongoDb stream");
            stream.close();
            stream = null;
        }
    }

    @Override
    @SuppressWarnings("java:S1168") // Spring-Batch requires us to return null when we're done
    protected List<Record> doRead() {
        List<Record> result = new ArrayList<>(settings.getBatchSize());
        Long start = System.currentTimeMillis();

        synchronized (iterator) {
            while (iterator.hasNext() && result.size() < settings.getBatchSize()) {
                result.add(iterator.next());
            }
        }

        if (!result.isEmpty()) {
            LOG.debug("1. Retrieved {} items from Mongo in {} ms", result.size(), System.currentTimeMillis() - start);
            return result;
        }
        LOG.info("Finished reading records from Mongo");
        return null;
    }



    // TODO implement better resume from error functionality

//    @Override
//    protected void jumpToItem(int itemIndex) {
//        iterable = iterable.skip(itemIndex);
//    }


}
