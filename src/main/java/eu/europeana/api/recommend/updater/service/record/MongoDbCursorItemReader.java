package eu.europeana.api.recommend.updater.service.record;

import eu.europeana.api.recommend.updater.config.JobCmdLineStarter;
import eu.europeana.api.recommend.updater.config.JobData;
import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import eu.europeana.api.recommend.updater.model.record.Record;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.*;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

/**
 * Spring Batch reader for reading CHO records from a Mongo database. Reading is done with 1 cursor per set
 * Note that we check the lastModified date of each record so we can check if things changed during the update. If so
 * we'll skip the changed record and log a warning.
 *
 * @author Patrick Ehlert
 */
@Service
public class MongoDbCursorItemReader extends AbstractItemCountingItemStreamItemReader<List<Record>> implements StepExecutionListener {

    private static final Logger LOG = LogManager.getLogger(MongoDbCursorItemReader.class);

    private static final String TOTAL_ITEMS = "TotalItems";

    private final UpdaterSettings settings;
    private final MongoRecordRepository mongoRecordRepository;

    private Date updateStart; // to check if records were modified during the update
    private Boolean isFullUpdate;
    private Date fromDate;

    // we create 1 cursor per set so we can download multiple sets at a time
    private final Queue<String> setsToDo = new ConcurrentLinkedQueue<>();
    private final Queue<SetCursor> setsInProgress = new ConcurrentLinkedQueue<>();

    public MongoDbCursorItemReader(UpdaterSettings settings, MongoRecordRepository mongoRecordRepository) {
        this.settings = settings;
        this.mongoRecordRepository = mongoRecordRepository;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        // check job parameters
        isFullUpdate = JobCmdLineStarter.isFullUpdate(stepExecution.getJobParameters());
        fromDate = JobCmdLineStarter.getFromDate(stepExecution.getJobParameters());
        Object sets = stepExecution.getJobExecution().getExecutionContext().get(JobData.SETS_KEY);
        if (sets instanceof List) {
            List<String> list = (List<String>) sets;
            setsToDo.addAll(list);
        }

        // get total record count from Mongo
        long nrRecordsToProcess;
        if (this.isFullUpdate) {
            nrRecordsToProcess = mongoRecordRepository.countAllBy();
        } else {
            nrRecordsToProcess = mongoRecordRepository.countAllByTimestampUpdatedAfter(this.fromDate);
            LOG.info("Total number of records to download {}", nrRecordsToProcess);
        }
        stepExecution.getExecutionContext().put(TOTAL_ITEMS, nrRecordsToProcess);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        // do nothing
        return null;
    }

    @Override
    protected void doOpen() {
        this.updateStart = new Date();
    }

    @PreDestroy
    @Override
    protected void doClose() {
        // TODO also close cursors in progress!?

        for (SetCursor setCursor : setsInProgress) {
            LOG.info("Closing Mongo cursor for set {}", setCursor.setId);
            setCursor.cursor.close();
        }
    }

    @Override
    @SuppressWarnings("java:S1168") // Spring-Batch requires us to return null when we're done
    protected List<Record> doRead() {

        // find work to do
        SetCursor setCursor = setsInProgress.poll();
        if (setCursor == null) {
            // create new work
            String newSet = setsToDo.poll();
            if (newSet == null) {
                LOG.debug("No more work");
                return null;
            }

            Stream<Record> newStream;
            LOG.info("Starting on set {}", newSet);
            if (isFullUpdate) {
                newStream = mongoRecordRepository.streamAllByAboutRegexOrderByAbout("^/" + newSet + "/");
            } else {
                newStream = mongoRecordRepository.streamAllByAboutRegexAndTimestampUpdatedAfterOrderByAbout("^/" + newSet + "/", fromDate);
            }
            setCursor = new SetCursor(newSet, newStream);
        } else {
            LOG.debug("Continue with set {}", setCursor.setId);
        }

        // fetch records for selected setCursor
        List<Record> result = new ArrayList<>(settings.getBatchSize());
        Long start = System.currentTimeMillis();

        while (setCursor.iterator.hasNext() && (result.size() < settings.getBatchSize())) {
            Record record = setCursor.iterator.next();
            LOG.trace("Read record {}", record.getAbout());
            if (updateStart.after(record.getTimestampUpdated())) {
                result.add(record);
            } else {
                LOG.warn("Record {} was updated after starting the update! Skipping it", record.getAbout());
            }
        }

        // check if set is done or not
        if (setCursor.iterator.hasNext()) {
            setsInProgress.add(setCursor); // put back in queue, still work to be done
        } else {
            LOG.info("Finished reading set {}", setCursor.setId);
            setCursor.cursor.close();
        }

        // return result
        if (!result.isEmpty()) {
            LOG.debug("1. Retrieved {} items from Mongo in {} ms", result.size(), System.currentTimeMillis() - start);
            return result;
        }

        // TODO think if we need additional checks, what should we return what we should return here.
        LOG.debug("setsInProgress = {}", setsInProgress);
        LOG.info("Done reading from Mongo!");
        return null;
    }


    // keep track of the open cursors, one for each set
    private static final class SetCursor {
        private final String setId;
        private final Stream<Record> cursor;
        private Iterator<Record> iterator;

        private SetCursor(String setId, Stream<Record> cursor) {
            this.setId = setId;
            this.cursor = cursor;
            this.iterator = cursor.iterator();
        }

    }

}
