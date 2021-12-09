package eu.europeana.api.recommend.updater.config;

import eu.europeana.api.recommend.updater.exception.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import javax.annotation.PostConstruct;

/**
 * Container for all settings that we load from the application's properties file and optionally override from
 * the user.properties file.
 */
@Configuration
@PropertySource("classpath:recommend.updater.properties")
@PropertySource(value = "classpath:recommend.updater.user.properties", ignoreResourceNotFound = true)
public class UpdaterSettings {

    private static final Logger LOG = LogManager.getLogger(UpdaterSettings.class);

    private static final int DEFAULT_BATCH_SIZE = 20;

    @Value("${record.batchSize:}")
    private Integer batchSize;

    @Value("${embeddings.api.url:}")
    private String embeddingsApiUrl;

    @Value("${milvus.url:}")
    private String milvusUrl;
    @Value("${milvus.collection:}")
    private String milvusCollection;

    @Value("${test.file:}")
    private String testFile;

    @PostConstruct
    private void logImportantSettings() throws ConfigurationException {
        LOG.info("Configuration:");
        LOG.info("  Batch size = {}", getBatchSize());
        LOG.info("  Embeddings API = {}", embeddingsApiUrl);
        LOG.info("  Milvus {} at {}", milvusUrl, milvusCollection);
        LOG.info("  Test file {}", testFile);

        if (isValueDefined(milvusUrl) && !isValueDefined(milvusCollection)) {
            throw new ConfigurationException("Property milvus.collection is required when milvus.url is defined");
        }
    }

    /**
     * Check if a configuration property is defined
     * @param value property to check
     * @return true if value doesn't exist, is blank or equals [REMOVED]
     */
    public static boolean isValueDefined(String value) {
        return !(value == null || StringUtils.isBlank(value) || StringUtils.equalsIgnoreCase(value.trim(), "[REMOVED]"));
    }

    public Integer getBatchSize() {
        if (batchSize == null) {
            return DEFAULT_BATCH_SIZE;
        }
        return batchSize;
    }

    public String getEmbeddingsApiUrl() {
        return embeddingsApiUrl;
    }

    public String getMilvusUrl() {
        return milvusUrl;
    }

    public String getMilvusCollection() {
        return milvusCollection;
    }

    public String getTestFile() {
        return testFile;
    }
}
