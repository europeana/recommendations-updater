package eu.europeana.api.recommend.updater.config;

import eu.europeana.api.recommend.updater.exception.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;

/**
 * Processes command-line parameters that determine what kind of update to start; either a full update
 * (indicated by the --FULL parameter) or a partial update (which requires a 'from' parameter with a date
 * selecting all records that were created or updated after this date).
 *
 * Additionally the --DELETE option can be provided which deletes any existing Milvus (and LMDB) data.
 *
 * @author Patrick Ehlert
 */
@Configuration
@SuppressWarnings("java:S1147") // we call System.exit() on purpose as this is a command-line application
public class JobCmdLineStarter implements ApplicationRunner {

    public static final String PARAM_UPDATE_FULL = JobData.UPDATETYPE_VALUE_FULL.toUpperCase(Locale.ROOT);
    public static final String PARAM_UPDATE_FROM = JobData.FROM_KEY;
    public static final String PARAM_UPDATE_SETS = JobData.SETS_KEY;
    public static final String PARAM_DELETE_DB = JobData.DELETE_DB.toUpperCase(Locale.ROOT);

    private static final Logger LOG = LogManager.getLogger(JobCmdLineStarter.class);

    private static final String SET_SEPARATOR = ",";

    private static final String FULL_DESCRIPTION = "'--" + PARAM_UPDATE_FULL + "' parameter";
    private static final String PARTIAL_DESCRIPTION = "'--" + PARAM_UPDATE_FROM + "=[yyyy-MM-ddThh:mm:ss]' parameter and date value";
    private static final String SETS_DESCRIPTION = "'--" + PARAM_UPDATE_SETS + "=<setIds>' parameter and comma-separated set-id values";

    private final JobLauncher jobLauncher;
    private final Job job;

    public JobCmdLineStarter(JobLauncher jobLauncher, Job job) {
        this.jobLauncher = jobLauncher;
        this.job = job;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (LOG.isInfoEnabled()) {
            LOG.info("Application started with command-line params {}", Arrays.toString(args.getSourceArgs()));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Command-line non-options = {}", args.getNonOptionArgs());
            for (String optionName : args.getOptionNames()) {
                LOG.debug("Command-line option {} = {}", optionName, args.getOptionValues(optionName));
            }
        }

        JobParametersBuilder jobParamBuilder = new JobParametersBuilder();
        if (args.getOptionNames().contains(PARAM_UPDATE_FULL)) {
            processFullUpdate(args, jobParamBuilder);
        } else if (args.getOptionNames().contains(PARAM_UPDATE_FROM)) {
            processPartialUpdate(args, jobParamBuilder);
        } else if (args.getOptionNames().contains(PARAM_UPDATE_SETS)) {
            processSets(args, jobParamBuilder);
        } else {
            throw new ConfigurationException("Specify either command-line " + FULL_DESCRIPTION +
                    ", " + PARTIAL_DESCRIPTION +
                    " or " + SETS_DESCRIPTION);
        }

        if (args.getOptionNames().contains(PARAM_DELETE_DB)) {
            processDelete(jobParamBuilder);
        }

        // For testing purposes we use the --test option. In this case we don't start the job
        if (args.getNonOptionArgs().contains("test")) {
            LOG.info("--test option detected. Not starting a job");
        } else {
            LOG.info("Starting job {}...", job.getName());
            JobExecution execution = jobLauncher.run(job, jobParamBuilder.toJobParameters());
            LOG.info("Job finished with status {}", execution.getStatus());
            System.exit(execution.getStatus().ordinal());
        }
    }

    private void processFullUpdate(ApplicationArguments args, JobParametersBuilder jobParamBuilder) throws ConfigurationException {
        if (args.getOptionNames().contains(PARAM_UPDATE_FROM)) {
            throw new ConfigurationException("Both full and partial update arguments found. Please specify either the " +
                    FULL_DESCRIPTION + " or a " + PARTIAL_DESCRIPTION);
        }
        jobParamBuilder.addString(JobData.UPDATETYPE_KEY, JobData.UPDATETYPE_VALUE_FULL);
    }

    private void processPartialUpdate(ApplicationArguments args, JobParametersBuilder jobParamBuilder) throws ConfigurationException {
        jobParamBuilder.addString(JobData.UPDATETYPE_KEY, JobData.UPDATETYPE_VALUE_PARTIAL);
        List<String> fromDate = args.getOptionValues(PARAM_UPDATE_FROM);
        if (fromDate == null || fromDate.isEmpty() || StringUtils.isBlank(fromDate.get(0))) {
            throw new ConfigurationException("Please specify a " + PARTIAL_DESCRIPTION);
        }
        // parse either date or dateTime
        Date date;
        if (fromDate.get(0).contains("T")) {
            LocalDateTime from = LocalDateTime.parse(fromDate.get(0), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            date = Date.from(from.atZone(ZoneId.systemDefault()).toInstant());
        } else {
            LocalDate from = LocalDate.parse(fromDate.get(0), DateTimeFormatter.ISO_LOCAL_DATE);
            date = Date.from(from.atStartOfDay(ZoneId.systemDefault()).toInstant());
        }
        jobParamBuilder.addDate(JobData.FROM_KEY, date);
    }

    private void processSets(ApplicationArguments args, JobParametersBuilder jobParamBuilder) throws ConfigurationException {
        jobParamBuilder.addString(JobData.UPDATETYPE_KEY, JobData.UPDATETYPE_VALUE_PARTIAL);
        List<String> setIds = args.getOptionValues(PARAM_UPDATE_SETS);

        // validate
        String ids = setIds.get(0);
        if (StringUtils.isBlank(ids)) {
            throw new ConfigurationException( PARAM_UPDATE_SETS + " parameter found, but no set ids provided!");
        }

        jobParamBuilder.addString(JobData.SETS_KEY, ids);
    }

    private void processDelete(JobParametersBuilder jobParametersBuilder) {
        jobParametersBuilder.addString(JobData.DELETE_DB, "true");
    }


    public static boolean isFullUpdate(JobParameters jobParameters) {
        return PARAM_UPDATE_FULL.equalsIgnoreCase(jobParameters.getString(JobData.UPDATETYPE_KEY));
    }

    public static Date getFromDate(JobParameters jobParameters) {
        return jobParameters.getDate(JobData.FROM_KEY);
    }

    public static boolean isDeleteDb(JobParameters jobParameters) {
        return Boolean.valueOf(jobParameters.getString(JobData.DELETE_DB));
    }

    public static List<String> getSetsToProcess(JobParameters jobParameters) {
        String sets = jobParameters.getString(JobData.SETS_KEY);
        if (StringUtils.isBlank(sets)) {
            return null;
        }
        return Arrays.asList(sets.split(SET_SEPARATOR));
    }
}
