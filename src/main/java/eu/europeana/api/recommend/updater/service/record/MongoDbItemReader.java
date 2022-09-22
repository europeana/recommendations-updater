package eu.europeana.api.recommend.updater.service.record;

import eu.europeana.api.recommend.updater.config.JobCmdLineStarter;
import eu.europeana.api.recommend.updater.config.JobData;
import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import eu.europeana.api.recommend.updater.model.record.Record;
import eu.europeana.api.recommend.updater.util.AverageTime;
import eu.europeana.api.recommend.updater.util.ProgressLogger;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
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

    private static final String RESULT_FILE_NAME = "UpdateResults";
    private static final String RESULT_FILE_EXTENSION = ".csv";
    private static final char SEPARATOR = ';';

    private final UpdaterSettings settings;
    private final MongoService mongoService;
    private final FileWriter resultsFile;
    private final String resultsFileName;
    private final BufferedWriter bufferedResultWriter;

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
    public MongoDbItemReader(UpdaterSettings settings, MongoService mongoService) throws IOException {
        this.settings = settings;
        this.mongoService = mongoService;
        this.averageTime = new AverageTime(settings.getLogTimingInterval(), "reading from Mongo");

        if (StringUtils.isBlank(settings.getMilvusCollection())) {
            this.resultsFileName = RESULT_FILE_NAME + RESULT_FILE_EXTENSION;
        } else {
            this.resultsFileName = RESULT_FILE_NAME + "-" + settings.getMilvusCollection() + RESULT_FILE_EXTENSION;
        }
        this.resultsFile = new FileWriter(resultsFileName, true);
        this.bufferedResultWriter = new BufferedWriter(resultsFile);
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
        this.writeHeader(totalItemsToRead);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        // do nothing
        return stepExecution.getExitStatus();
    }

    @Override
    protected void doOpen() {
        this.updateStart = new Date();
        // prepare sets to process
        for (int i = 0; i < settings.getThreads(); i++) {
            addSetInProgress();
        }
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
        SetInProgress setToProcess = getSetInProgress();
        if (setToProcess == null) {
            return null;
        }
        if (setToProcess.itemsRead == 0) {
            LOG.trace("Start reading set {}", setToProcess.setId);
        }

        // fetch records for selected setCursor
        long start = System.currentTimeMillis();
        List<Record> result;
        if (isFullUpdate || this.fromDate == null) {
            result = mongoService.getAllRecordsPaged(setToProcess.regex, setToProcess.lastRetrieved, settings.getBatchSize());
        } else {
            result = mongoService.getAllRecordsPagedUpdatedAfter(setToProcess.regex, fromDate, setToProcess.lastRetrieved, settings.getBatchSize());
        }
        boolean setDone = result.isEmpty() || result.size() < settings.getBatchSize();

        if (LOG.isTraceEnabled()) {
            LOG.trace("1. Retrieved {} items from set {} in {} ms", result.size(), setToProcess.setId, System.currentTimeMillis() - start);
        }
        if (LOG.isDebugEnabled()) {
            averageTime.addTiming(System.currentTimeMillis() - start);
        }
        if (!isFullUpdate) {
            result = checkTimestamp(result); // probably not needed, but just in case
        }

        if (!result.isEmpty()) {
            // more work to do, so keep set in progress (and update stats)
            setToProcess.itemsRead = setToProcess.itemsRead + result.size();
            setToProcess.lastRetrieved = result.get(result.size() - 1).getMongoId();
            if (!setDone) {
                setsInProgress.add(setToProcess); // put back in queue, still work to be done
            }
            progressLogger.logProgress(result.size());
        }

        if (setDone) {
            // work is done for this set, setInProgress doesn't go back in queue
            if (setToProcess.itemsRead == 0) {
                // Check if the set exists. It may have been deleted in the mean time, or the user provided an incorrect set name
                long nrItemsInSet = mongoService.countAllAboutRegex("^/" + setToProcess.setId + "/");
                if (nrItemsInSet == 0) {
                    LOG.warn("No items found for set {}!", setToProcess.setId);
                } else {
                    LOG.error("No items read for set {}, but set has {} items!", setToProcess.setId, nrItemsInSet);
                }
            } else {
                LOG.info("Finished reading set {}, retrieved {} items", setToProcess.setId, setToProcess.itemsRead);
            }

            writeResultToFile(setToProcess, new Date());
            addSetInProgress();
        }
        return result;
    }

    private synchronized  void writeHeader(long itemsToRead) {
        try {
            this.bufferedResultWriter.write("SetId" + SEPARATOR
                    + "Items read" + SEPARATOR
                    + "Start date" + SEPARATOR
                    + "End date" + SEPARATOR
                    + "Records to download " + itemsToRead);
            this.bufferedResultWriter.newLine();
            this.bufferedResultWriter.flush();
        } catch (IOException e) {
            LOG.error("Error writing result file header", e);
        }
    }

    /**
     * Whenever a set is done we write to csv file what was done.
     */
    private synchronized void writeResultToFile(SetInProgress setData, Date dateDone) {
        try {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
            bufferedResultWriter.write(setData.setId + SEPARATOR
                    + setData.itemsRead + SEPARATOR
                    + df.format(setData.started) + SEPARATOR
                    + (dateDone == null ? "null" : df.format(dateDone)));
            bufferedResultWriter.newLine();
            bufferedResultWriter.flush();
        } catch (IOException e) {
            LOG.error("Error writing to result file {}", this.resultsFileName, e);
        }
    }

    /**
     * Move a set from the 'to do' queue to 'in progress'
     * @return the id of the set that was moved, or null if there was none.
     */
    private String addSetInProgress() {
        String newSetId = setsToDo.poll();
        if (newSetId == null) {
            LOG.debug("No more sets to process");
            return null;
        }
        SetInProgress newSet = new SetInProgress(newSetId);
        setsInProgress.add(newSet);
        LOG.info("Starting on new set {}", newSet.setId);
        return newSet.setId;
    }

    /**
     * Pick a set from the 'in progress' queue
     */
    private SetInProgress getSetInProgress() {
        SetInProgress result = setsInProgress.poll();
        if (result == null) {
            String newSet = addSetInProgress();
            if (newSet == null) {
                LOG.info("No more sets in progress. Stopping thread");
            } else {
                // Should not happen. New sets should be added to the queue when previous one finished
                LOG.error("No sets in progress, adding new set {}", newSet);
            }
        } else {
            LOG.trace("Continue with set {}, skip = {}", result.setId, result.itemsRead);
        }
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
        private final Date started;
        private String lastRetrieved;
        private long itemsRead;

        private SetInProgress(String setId) {
            this.setId = setId;
            this.regex = "^/" + setId + "/";
            this.started = new Date();
            this.lastRetrieved = null;
            this.itemsRead = 0;
        }

    }

    @PreDestroy
    public void shutDown() {
        // Try to write to file in case the application is shutdown because of kill signal or of error
        for (SetInProgress set : setsInProgress) {
            LOG.error("Processing set {} did not finish properly. {} items were read", set.setId, set.itemsRead);
            writeResultToFile(set, null);
        }
        // Close file writer
        try {
            LOG.info("Closing result file writer...");
            if (bufferedResultWriter != null) {
                bufferedResultWriter.close();
            }
            if (resultsFile != null) {
                resultsFile.close();
            }
        } catch (IOException e) {
            LOG.error("Error closing result file writer", e);
        }
    }
}
