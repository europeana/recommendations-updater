package eu.europeana.api.recommend.updater.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.Duration;
import org.joda.time.Period;

/**
 * Utility class to log processing progress after roughly ever x seconds.
 * @author Patrick Ehlert
 */
public class ProgressLogger {

    private static final Logger LOG = LogManager.getLogger(ProgressLogger.class);

    private static final int MS_PER_SEC = 1_000;
    private static final int SEC_PER_MIN = 60;

    private final long startTime;
    private final long totalItems;
    private long itemsDone;
    private final int logAfterSeconds;
    private long lastLogTime;

    /**
     * Create a new progressLogger. This also sets the operation start Time
     * @param totalItems total number of items that are expected to be retrieved
     * @param logAfterSeconds to prevent too much logging, only log every x seconds
     */
    public ProgressLogger(long totalItems, int logAfterSeconds) {
        this.startTime = System.currentTimeMillis();
        this.lastLogTime = startTime;
        this.totalItems = totalItems;
        this.itemsDone = 0;
        this.logAfterSeconds = logAfterSeconds;
    }

    /**
     * Log the number of items that were processed as well as an estimate of the remaining processing time, every x seconds
     * as specified by logAfterSeconds
     * @param newItemsProcessed the number of new items that were processed
     */
    public synchronized void logProgress(long newItemsProcessed) {
        this.itemsDone = itemsDone + newItemsProcessed;

        Duration d = new Duration(lastLogTime, System.currentTimeMillis());
        if (logAfterSeconds > 0 && d.getMillis() / MS_PER_SEC > logAfterSeconds) {
            if (totalItems > 0) {
                double itemsPerMS = itemsDone * 1D / (System.currentTimeMillis() - startTime);
                if (itemsPerMS * MS_PER_SEC > 1.0) {
                    LOG.info("Retrieved {} items of {} ({} records/sec). Expected time remaining is {}", itemsDone, totalItems,
                            Math.round(itemsPerMS * MS_PER_SEC), getDurationText(Math.round((totalItems - itemsDone) / itemsPerMS)));
                } else {
                    LOG.info("Retrieved {} items of {} ({} records/min). Expected time remaining is {}", itemsDone, totalItems,
                            Math.round(itemsPerMS * MS_PER_SEC * SEC_PER_MIN), getDurationText(Math.round((totalItems - itemsDone) / itemsPerMS)));
                }
            } else {
                LOG.info("Retrieved {} items", itemsDone);
            }
            lastLogTime = System.currentTimeMillis();
        }
    }

    /**
     * Log a duration in easy readable text
     * @param durationInMs duration to output
     * @return string containing duration in easy readable format
     */
    public static String getDurationText(long durationInMs) {
        String result;
        Period period = new Period(durationInMs);
        if (period.getDays() >= 1) {
            result = String.format("%d days, %d hours and %d minutes", period.getDays(), period.getHours(), period.getMinutes());
        } else if (period.getHours() >= 1) {
            result = String.format("%d hours and %d minutes", period.getHours(), period.getMinutes());
        } else if (period.getMinutes() >= 1){
            result = String.format("%d minutes and %d seconds", period.getMinutes(), period.getSeconds());
        } else {
            result = String.format("%d.%d seconds", period.getSeconds(), period.getMillis());
        }
        return result;
    }
}
