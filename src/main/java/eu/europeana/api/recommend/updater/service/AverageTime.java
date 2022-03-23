package eu.europeana.api.recommend.updater.service;


import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Tmp for testing. Outputs how long on average an x amount of operations took.
 */
public class AverageTime {

    private static final Logger LOG = LogManager.getLogger(AverageTime.class);

    private final int nrOperations;
    private final String operationName;

    private long currentTotal;
    private int currentNrOperations;

    /**
     * Create a new object for AverageTime measurement
     * @param nrOperations number of operations (calls to addTiming) after which the average duration is logged
     * @param operationName (optional), if present then this will be part of the log
     */
    public AverageTime(int nrOperations, String operationName) {
        this.nrOperations = nrOperations;
        this.operationName = operationName;
        this.currentTotal = 0;
        this.currentNrOperations = 0;
    }

    /**
     * Add how long a certain operation took.
     * @param timeInMS
     */
    public synchronized void addTiming(long timeInMS) {
        this.currentNrOperations++;
        this.currentTotal = currentTotal + timeInMS;
        if (this.currentNrOperations == nrOperations) {
            String msg = "Average time";
            if (!StringUtils.isBlank(operationName)) {
                msg = msg + " for " + operationName;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("{}: {}", msg, ProgressLogger.getDurationText(Math.round(currentTotal * 1D / currentNrOperations)));
            }
            // start new round of calculations
            this.currentNrOperations = 0;
            this.currentTotal = 0;
        }
    }

}
