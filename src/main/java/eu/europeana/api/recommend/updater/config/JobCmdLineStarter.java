package eu.europeana.api.recommend.updater.config;

import eu.europeana.api.recommend.updater.exception.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
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
 * (indicated by the FULL parameter) or a partial update (which requires a 'from' parameter with a date
 * selecting all records that were created or updated after this date).
 *
 * @author Patrick Ehlert
 */
@Configuration
public class JobCmdLineStarter implements ApplicationRunner {

    public static final String PARAM_UPDATE_FULL = "FULL";
    public static final String PARAM_UPDATE_PARTIAL = "from";

    public static final String JOB_UPDATETYPE_KEY = "updateType";
    public static final String JOB_UPDATETYPE_VALUE_FULL = PARAM_UPDATE_FULL.toLowerCase(Locale.ROOT);
    public static final String JOB_UPDATETYPE_VALUE_PARTIAL = "partial";
    public static final String JOB_FROM_KEY = PARAM_UPDATE_PARTIAL;


    private static final Logger LOG = LogManager.getLogger(JobCmdLineStarter.class);

    private static final String FULL_DESCRIPTION = "'" + PARAM_UPDATE_FULL + "' parameter";
    private static final String PARTIAL_DESCRIPTION = "'--" + PARAM_UPDATE_PARTIAL + "=[yyyy-MM-ddThh:mm:ss]' parameter and date value";

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
        if (args.getNonOptionArgs().contains(PARAM_UPDATE_FULL)) {
           processFullUpdate(args, jobParamBuilder);
        } else if (args.getOptionNames().contains(PARAM_UPDATE_PARTIAL)) {
            processPartialUpdate(args, jobParamBuilder);
        } else {
            throw new ConfigurationException("Either command-line " + FULL_DESCRIPTION + " or a " +
                    PARTIAL_DESCRIPTION + " needs to be provided");
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
        if (args.getOptionNames().contains(PARAM_UPDATE_PARTIAL)) {
            throw new ConfigurationException("Both full and partial update arguments found. Please specify either the " +
                    FULL_DESCRIPTION + " or a " + PARTIAL_DESCRIPTION);
        }
        jobParamBuilder.addString(JOB_UPDATETYPE_KEY, JOB_UPDATETYPE_VALUE_FULL);
    }

    private void processPartialUpdate(ApplicationArguments args, JobParametersBuilder jobParamBuilder) throws ConfigurationException {
        jobParamBuilder.addString(JOB_UPDATETYPE_KEY, JOB_UPDATETYPE_VALUE_PARTIAL);
        List<String> fromDate = args.getOptionValues(PARAM_UPDATE_PARTIAL);
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
        jobParamBuilder.addDate(JOB_FROM_KEY, date);
    }

}
