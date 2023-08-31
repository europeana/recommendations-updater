package eu.europeana.api.recommend.updater;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

/**
 * Main application
 */
@SpringBootApplication(scanBasePackages = "eu.europeana.api", exclude = {DataSourceAutoConfiguration.class})
public class RecommendUpdaterApplication {

    /**
     * Main entry point of this application
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(RecommendUpdaterApplication.class, args)));
    }

}
