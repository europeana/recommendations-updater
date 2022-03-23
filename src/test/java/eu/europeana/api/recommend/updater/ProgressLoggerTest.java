package eu.europeana.api.recommend.updater;

import eu.europeana.api.recommend.updater.service.ProgressLogger;
import nl.altindag.log.LogCaptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith(OutputCaptureExtension.class)
public class ProgressLoggerTest {

    @Test
    public void getDurationTextTest() {
        assertEquals("12 milliseconds", ProgressLogger.getDurationText(12));
        assertEquals("1012 milliseconds", ProgressLogger.getDurationText(1012));
        assertEquals("2 seconds and 3 milliseconds", ProgressLogger.getDurationText(2003));
        assertEquals("10 seconds", ProgressLogger.getDurationText(10012));
        int min = 60 * 1000;
        assertEquals("2 minutes and 3 seconds", ProgressLogger.getDurationText(2 * min + 3000));
        int hour = 60 * min;
        assertEquals("2 hours and 0 minutes", ProgressLogger.getDurationText(2 * hour));
        int day = 24 * hour;
        assertEquals("3 days, 2 hours and 7 minutes", ProgressLogger.getDurationText(3 * day + 2 * hour + 7 * min));
    }

    @Test
    public void logProgressTest(CapturedOutput output) throws InterruptedException {
        LogCaptor logCaptor = LogCaptor.forClass(ProgressLogger.class);

        ProgressLogger logger = new ProgressLogger(1000, 2);
        TimeUnit.MILLISECONDS.sleep(500);
        logger.logProgress(1);

        assertEquals(0, logCaptor.getInfoLogs().size()); // nothing should be logged yet

        TimeUnit.SECONDS.sleep(2);
        logger.logProgress(3);

        assertEquals(1, logCaptor.getInfoLogs().size());
        assertTrue(logCaptor.getInfoLogs().get(0).startsWith("Retrieved 4 items of 1000"));
    }

}
