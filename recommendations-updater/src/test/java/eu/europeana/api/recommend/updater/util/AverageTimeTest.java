package eu.europeana.api.recommend.updater.util;

import nl.altindag.log.LogCaptor;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AverageTimeTest {

    @Test
    public void averageTimeTest() {
        LogCaptor logCaptor = LogCaptor.forClass(AverageTime.class);
        logCaptor.setLogLevelToDebug();

        AverageTime avg = new AverageTime(10, null);
        for (int i = 0; i < 10; i++) {
            assertEquals(0, logCaptor.getDebugLogs().size()); // no logs yet
            avg.addTiming(1000 + i * 100);
        }

        assertEquals(1, logCaptor.getDebugLogs().size()); // should be one log now
        assertTrue(logCaptor.getDebugLogs().get(0).startsWith("Average time: 1450 milliseconds"));
    }

    @Test
    public void averageTimeTest2() {
        LogCaptor logCaptor = LogCaptor.forClass(AverageTime.class);
        logCaptor.setLogLevelToDebug();

        AverageTime avg = new AverageTime(10, "testing");
        for (int i = 0; i < 10; i++) {
            assertEquals(0, logCaptor.getDebugLogs().size()); // no logs yet
            avg.addTiming(100_000 + i * 100);
        }

        assertEquals(1, logCaptor.getDebugLogs().size()); // should be one log now
        assertTrue(logCaptor.getDebugLogs().get(0).startsWith("Average time for testing: 1 minutes and 40 seconds"));
    }
}
