package eu.europeana.api.recommend.updater.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StringLimitUtilsTest {

    @Test
    public void testStringLimit() {
        String s1 = "This is a test";
        assertEquals("This is a", StringLimitUtils.limit(s1, s1.length() - 1));
        assertEquals("This is a test", StringLimitUtils.limit(s1, s1.length() + 1));
        assertEquals("This", StringLimitUtils.limit(s1, 4));
        assertEquals("This", StringLimitUtils.limit(s1, 5));

        String s2 = "This is another test.";
        assertEquals("This is another test", StringLimitUtils.limit(s2, s2.length() - 1));

        String s3 = "This is a third test;12345";
        assertEquals("This is a third test", StringLimitUtils.limit(s3, s3.length() - 5));

        String s4 = "FinalTestWhereIt'sImpossibleToTruncateProperly";
        assertEquals("FinalTest", StringLimitUtils.limit(s4, "FinalTest".length()));
    }
}
