package eu.europeana.api.recommend.updater.service;

import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * Sends an email when the update finished
 */
@Service
public class MailService implements JobExecutionListener {

    private static final Logger LOG = LogManager.getLogger(MailService.class);
    private static final String FROM = "Recommendations Updater <noreply@europeana.eu>";

    private JavaMailSender mailSender;
    private String mailTo;

    /**
     * Setup new Mail service
     * @param mailSender auto-wired Java mail sender
     * @param settings auto-wired settings
     */
    public MailService(JavaMailSender mailSender, UpdaterSettings settings) {
        this.mailSender = mailSender;
        this.mailTo = settings.getMailTo();
    }

    public void beforeJob(JobExecution jobExecution) {
        // do nothing
    }

    public void afterJob(JobExecution jobExecution) {
        sendMail(jobExecution);
    }

    private void sendMail(JobExecution jobExecution) {
        if (StringUtils.isNotBlank(mailTo)) {
            SimpleMailMessage message = new SimpleMailMessage();

            message.setFrom(FROM);
            message.setTo(mailTo);

            SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm", Locale.getDefault());
            String text = "Recommendations Updater running on " + getHostName()
                    + " finished with status " + jobExecution.getExitStatus().getExitCode();
            message.setSubject(text);

            StringBuilder s = new StringBuilder(text);
            s.append("\nTime: " + sdf.format(jobExecution.getEndTime()));
            for (Throwable t : jobExecution.getAllFailureExceptions()) {
                s.append("\nError: ");
                Throwable cause = t.getCause();
                if (cause == null) {
                    s.append("n/a");
                } else {
                    s.append(cause.getMessage()).append("\nCaused by").append(cause.getCause());
                }
            }
            message.setText(s.toString());

            LOG.info("Sending email to {}..", mailTo);
            mailSender.send(message);
        } else {
            LOG.info("Not sending email. No mail.to address is configured");
        }
    }

    @SuppressWarnings("fb-contrib:MDM_INETADDRESS_GETLOCALHOST") // not important, won't expose this info to the outside
    private String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOG.warn("Cannot read host name", e);
            return "Unknown host";
        }

    }

}
