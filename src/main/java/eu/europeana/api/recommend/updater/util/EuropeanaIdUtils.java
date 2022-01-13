package eu.europeana.api.recommend.updater.util;

/**
 * Utility class to handle EuropeanaIds
 */
public final class EuropeanaIdUtils {

    public EuropeanaIdUtils() {
        // empty constructor to avoid initialization
    }

    public static String getSetName(String europeanaId) {
        return europeanaId.split("/")[1];
    }
}
