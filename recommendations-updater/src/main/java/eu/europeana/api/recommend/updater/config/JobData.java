package eu.europeana.api.recommend.updater.config;

/**
 * Define fields used in JobExecution context
 *
 * @author Patrick Ehlert
 */
public final class JobData {

    public static final String UPDATETYPE_KEY = "updateType";
    public static final String UPDATETYPE_VALUE_FULL = "full";
    public static final String UPDATETYPE_VALUE_PARTIAL = "partial";

    public static final String FROM_KEY = "from";

    public static final String SETS_KEY = "sets";

    public static final String SETSFILE_KEY = "setsFile";

    public static final String DELETE_DB = "delete";

    private JobData() {
        // empty constructor to prevent initialization
    }
}
