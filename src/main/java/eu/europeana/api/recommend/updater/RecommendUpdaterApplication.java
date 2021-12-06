package eu.europeana.api.recommend.updater;

import eu.europeana.api.recommend.updater.config.SocksProxyConfig;
import eu.europeana.api.recommend.updater.util.SocksProxyActivator;
import org.apache.logging.log4j.LogManager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

/**
 * Main application
 */
@SpringBootApplication(scanBasePackages = "eu.europeana.api") //, exclude = {MongoAutoConfiguration.class})
public class RecommendUpdaterApplication extends SpringBootServletInitializer {

    /**
     * Main entry point of this application
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        // When deploying to Cloud Foundry, this will log the instance index number, IP and GUID
        LogManager.getLogger(RecommendUpdaterApplication.class).
                info("CF_INSTANCE_INDEX  = {}, CF_INSTANCE_GUID = {}, CF_INSTANCE_IP  = {}",
                    System.getenv("CF_INSTANCE_INDEX"),
                    System.getenv("CF_INSTANCE_GUID"),
                    System.getenv("CF_INSTANCE_IP"));

        // Activate socks proxy (if your application requires it)
        SocksProxyActivator.activate(new SocksProxyConfig("recommend.update.properties", "recommend.updater.user.properties"));

        // TODO validate command-line args before passing on, or pass on null instead
        System.exit(SpringApplication.exit(SpringApplication.run(RecommendUpdaterApplication.class, args)));
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(RecommendUpdaterApplication.class);
    }

}
