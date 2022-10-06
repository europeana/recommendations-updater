package eu.europeana.api.recommend.updater.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Utility class to truncate String, but keep entire words
 */
public class StringLimitUtils {

    private StringLimitUtils() {
        // empty constructor to prevent initialization
    }

    /**
     * Limit string to a certain maximum number of characters and excluding partial words
     * @param s the string to truncate (if necessary)
     * @param maxChars the maximum allowed characters in the string
     * @return truncated string
     */
    public static String limit(String s, int maxChars) {
        if (s.length() > maxChars) {
            String sTrim = s.substring(0, maxChars);

            // Remove partial words (if possible)
            if (s.charAt(maxChars) != ' ' && s.charAt(maxChars) != ','
                    && s.charAt(maxChars) != ';' && s.charAt(maxChars) != '.') {
                int lastDelimiter = Math.max(sTrim.lastIndexOf(' '), Math.max(sTrim.lastIndexOf(','),
                        Math.max(sTrim.lastIndexOf(';'), sTrim.lastIndexOf('.'))));
                if (lastDelimiter > 0) {
                    sTrim = s.substring(0, lastDelimiter);
                }
            }
            return sTrim.trim();
        }
        return s;
    }
}
