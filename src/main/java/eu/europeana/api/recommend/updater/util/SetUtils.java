package eu.europeana.api.recommend.updater.util;

/**
 * Author: Patrick Ehlert
 */
public final class SetUtils {

    private SetUtils() {
        // prevent initialization
    }

    public static String datasetNameToId(String datasetName) {
        if (datasetName.contains("_")) {
            // get all numbers for the first non-digit character. This also prevents issues with set names such as 0123a_myset
            return datasetName.replaceAll("(\\d*).+", "$1");
        }
        return datasetName;
    }
}
