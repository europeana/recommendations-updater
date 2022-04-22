package eu.europeana.api.recommend.updater.service.record;

import eu.europeana.api.recommend.updater.config.JobCmdLineStarter;
import eu.europeana.api.recommend.updater.config.JobData;
import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import eu.europeana.api.recommend.updater.model.record.Record;
import eu.europeana.api.recommend.updater.util.AverageTime;
import eu.europeana.api.recommend.updater.util.ProgressLogger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Spring Batch reader for reading CHO records from a Mongo database. Reading is done with 1 thread per set
 * Note that we check the lastModified date of each record so we can check if things changed during the update. If so
 * we'll skip the changed record and log a warning.
 *
 * This service also keeps track of progress because it's the only component that is always available (others may not
 * depending on the configuration)
 * @author Patrick Ehlert
 */
@Service
@SuppressWarnings("fb-contrib:USFW_UNSYNCHRONIZED_SINGLETON_FIELD_WRITES") // due to the way Spring-Batch works there
// is no need to synchronize changing instance variables in doOpen or doClose methods
public class MongoDbItemReader extends AbstractItemCountingItemStreamItemReader<List<Record>> implements StepExecutionListener {

    private static final Logger LOG = LogManager.getLogger(MongoDbItemReader.class);

    private final UpdaterSettings settings;
    private final MongoService mongoService;

    private Date updateStart; // to check if records were modified during the update
    private Boolean isFullUpdate;
    private Date fromDate;

    // we create 1 cursor per set so we can download multiple sets at a time
    private final Queue<String> setsToDo = new ConcurrentLinkedQueue<>();
    private final Queue<SetInProgress> setsInProgress = new ConcurrentLinkedQueue<>();

    // we add a time-based progresslogger here since this is the only component that is guaranteed to exist (others may
    // not depending on the configuration)
    private ProgressLogger progressLogger;
    private AverageTime averageTime; // for debugging purposes

    /**
     * Create new ItemReader that reads records from MongoDb
     * @param settings inject application settings bean
     * @param mongoService inject mongo service bean
     */
    public MongoDbItemReader(UpdaterSettings settings, MongoService mongoService) {
        this.settings = settings;
        this.mongoService = mongoService;
        this.averageTime = new AverageTime(settings.getLogTimingInterval(), "reading from Mongo");
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
        long totalItemsToRead = 0;
        if (this.isFullUpdate) {
            totalItemsToRead = mongoService.countAll();
        } else if (this.fromDate != null) {
            totalItemsToRead = mongoService.countAllUpdatedAfter(this.fromDate);
        } else {
            StringBuilder s = new StringBuilder();
            for (String setId : setsToDo) {
                s.append("^/").append(setId).append("/").append("|");
            }
            String setsRegex = s.substring(0, s.length() - 1);
            totalItemsToRead = mongoService.countAllAboutRegex(setsRegex);
        }
        LOG.info("Total number of records to download {}", totalItemsToRead);
        this.progressLogger = new ProgressLogger(totalItemsToRead, settings.getLogProgressInterval());
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
        LOG.debug("Application is shutting down...");
    }

    @Override
    // Spring-Batch requires us to return null when we're done (S1168)
    // The start variable needs to be where it is, cannot be moved (S1941)
    @SuppressWarnings({"java:S1168", "java:S1941" })
    protected List<Record> doRead() {
        SetInProgress setCursor = findWork();
        if (setCursor == null) {
            return null;
        }

        // fetch records for selected setCursor
        long start = System.currentTimeMillis();
        List<Record> result;
        if (isFullUpdate || this.fromDate == null) {
            result = mongoService.getAllRecordsPaged(setCursor.regex, setCursor.lastRetrieved, settings.getBatchSize());
        } else {
            result = mongoService.getAllRecordsPagedUpdatedAfter(setCursor.regex, fromDate, setCursor.lastRetrieved, settings.getBatchSize());
        }
        boolean setDone = result.isEmpty() || result.size() < settings.getBatchSize();

        if (LOG.isTraceEnabled()) {
            LOG.trace("1. Retrieved {} items from set {} in {} ms", result.size(), setCursor.setId, System.currentTimeMillis() - start);
        }
        if (LOG.isDebugEnabled()) {
            averageTime.addTiming(System.currentTimeMillis() - start);
        }
        if (!isFullUpdate) {
            result = checkTimestamp(result); // probably not needed, but just in case
        }

        // return result
        if (!result.isEmpty()) {
            setCursor.itemsRead = setCursor.itemsRead + result.size();
            setCursor.lastRetrieved = result.get(result.size() - 1).getMongoId();
            if (!setDone) {
                setsInProgress.add(setCursor); // put back in queue, still work to be done
            }
            progressLogger.logProgress(result.size());
        }
        if (setDone) {
            LOG.info("Finished with set {}, retrieved {} items", setCursor.setId, setCursor.itemsRead);
        }
        return result;
    }

    private SetInProgress findWork() {
        SetInProgress result = setsInProgress.poll();
        if (result == null) {
            // create new work
            String newSet = setsToDo.poll();
            if (newSet == null) {
                LOG.info("No more work. Stopping thread");
                return null;
            }

            LOG.info("Starting on set {}", newSet);
            return new SetInProgress(newSet);
        }

        LOG.trace("Continue with set {}, skip = {}", result.setId, result.itemsRead);
        return result;
    }

    private List<Record> checkTimestamp(List<Record> list) {
        List<Record> result = new ArrayList<>();
        for (Record record : list) {
            if (updateStart.after(record.getTimestampUpdated())) {
                result.add(record);
            } else {
                LOG.warn("Record {} was updated after starting the update! Skipping it", record.getAbout());
            }
        }
        return result;
    }

    // keep track of sets that are being downloaded
    @SuppressWarnings("fb-contrib:FCBL_FIELD_COULD_BE_LOCAL")
    private static final class SetInProgress {
        private final String setId;
        private final String regex;
        private String lastRetrieved;
        private long itemsRead;

        private SetInProgress(String setId) {
            this.setId = setId;
            this.regex = "^/" + setId + "/";
            this.lastRetrieved = null;
            this.itemsRead = 0;
        }

    }

}
