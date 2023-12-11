package eu.europeana.api.recommend.updater.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;

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
        long now = System.currentTimeMillis();
        this.itemsDone = itemsDone + newItemsProcessed;
        Duration d = Duration.ofMillis(now - lastLogTime);

        if (logAfterSeconds > 0 && d.getSeconds() >= logAfterSeconds) {
            if (totalItems > 0) {
                double itemsPerMS = itemsDone * 1D / (now - startTime);
                if (itemsPerMS * MS_PER_SEC * SEC_PER_MIN > 960.0) {
                    LOG.info("Retrieved {} items of {} ({} records/sec). Expected time remaining is {}", itemsDone, totalItems,
                            Math.round(itemsPerMS * MS_PER_SEC), getDurationText(Math.round((totalItems - itemsDone) / itemsPerMS)));
                } else {
                    LOG.info("Retrieved {} items of {} ({} records/min). Expected time remaining is {}", itemsDone, totalItems,
                            Math.round(itemsPerMS * MS_PER_SEC * SEC_PER_MIN), getDurationText(Math.round((totalItems - itemsDone) / itemsPerMS)));
                }
            } else {
                LOG.info("Retrieved {} items", itemsDone);
            }
            lastLogTime = now;
        }
    }

    /**
     * Log a duration in easy readable text
     * @param durationInMs duration to output
     * @return string containing duration in easy readable format
     */
    public static String getDurationText(long durationInMs) {
        String result;
        Duration d = Duration.ofMillis(durationInMs);
        if (d.toDaysPart() >= 1) {
            result = String.format("%d days, %d hours and %d minutes", d.toDaysPart(), d.toHoursPart(), d.toMinutesPart());
        } else if (d.toHoursPart() >= 1) {
            result = String.format("%d hours and %d minutes", d.toHoursPart(), d.toMinutesPart());
        } else if (d.toMinutesPart() >= 1) {
            result = String.format("%d minutes and %d seconds", d.toMinutesPart(), d.toSecondsPart());
        } else if (d.getSeconds() >= 10) {
            result = String.format("%d seconds", d.toSeconds());
        } else if (d.getSeconds() >= 2){
            result = String.format("%d seconds and %d milliseconds", d.toSecondsPart(), d.toMillisPart());
        } else {
            result = String.format("%d milliseconds", d.toMillis());
        }
        return result;
    }
}
