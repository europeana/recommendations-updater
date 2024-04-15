package eu.europeana.api.recommend.updater.config;

import eu.europeana.api.recommend.updater.exception.ConfigurationException;
import eu.europeana.api.recommend.common.model.EmbeddingRequestData;
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

    private static final int MAX_BATCH_SIZE = 500;
    private static final int DEFAULT_BATCH_SIZE = 200;
    private static final int DEFAULT_THREADS = 10;

    @Value("${batchSize:}")
    private Integer batchSize;
    @Value("${threads:}")
    private Integer threads;
    @Value("${log.progress.interval:300}")
    private Integer logProgressInterval;
    @Value("${log.debug.timing.interval:200}")
    private Integer logTimingInterval;

    @Value("${embedding.api.url:}")
    private String embeddingApiUrl;
    @Value("${embedding.api.timeout:60}")
    private Integer embeddingApiTimeout;

    @Value("${milvus.url:}")
    private String milvusUrl;
    @Value("${milvus.port}")
    private Integer milvusPort;
    @Value("${milvus.collection:}")
    private String milvusCollection;

    @Value("${milvus.collectionDescription:}")
    private String milvusCollectionDescription;
    @Value("${milvus.usePartitions:false}")
    private boolean useMilvusPartitions;

    @Value("${test.file:}")
    private String testFile;

    @Value("${mail.to:}")
    private String mailTo;

    @PostConstruct
    private void logImportantSettings() throws ConfigurationException {
        if (this.batchSize != null && this.batchSize > MAX_BATCH_SIZE) {
            LOG.warn("Batch size higher than {} is not allowed", MAX_BATCH_SIZE);
            this.batchSize = MAX_BATCH_SIZE;
        }

        LOG.info("Configuration:");
        LOG.info("  Batch size = {}", getBatchSize());
        LOG.info("  Threads = {}", getThreads());
        LOG.info("  Log interval = {} seconds", logProgressInterval);
        LOG.info("  Embeddings API = {}", embeddingApiUrl);
        LOG.info("    Timeout = {} sec", embeddingApiTimeout);
        LOG.info("    Reduce = {}", EmbeddingRequestData.REDUCE);
        LOG.info("  Milvus {} at {}", milvusUrl, milvusCollection);
        LOG.info("    Milvus use partitions = {}", useMilvusPartitions);
        LOG.info("  Test file {}", testFile);

        if (isValueDefined(milvusUrl)) {
            if (!isValueDefined(milvusCollection)) {
                throw new ConfigurationException("Property milvus.collection is required when milvus.url is defined");
            }
            if (milvusPort == null) {
                throw new ConfigurationException("Property milvus.port is required when milvus.url is defined");
            }
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

    public Integer getThreads() {
        if (threads == null) {
            return DEFAULT_THREADS;
        }
        return threads;
    }

    public Integer getLogProgressInterval() {
        return logProgressInterval;
    }

    public Integer getLogTimingInterval() {
        return logTimingInterval;
    }

    public String getEmbeddingApiUrl() {
        return embeddingApiUrl;
    }

    public Integer getEmbeddingApiTimeout() {
        return embeddingApiTimeout;
    }

    public String getMilvusUrl() {
        return milvusUrl;
    }

    public Integer getMilvusPort() {
        return milvusPort;
    }

    public String getMilvusCollection() {
        return milvusCollection;
    }

    public String getMilvusCollectionDescription() {
        return milvusCollectionDescription;
    }

    /**
     *
     * @return if true then milvus should create a partition for each set
     */
    public boolean useMilvusPartitions() {
        return useMilvusPartitions;
    }

    public String getTestFile() {
        return testFile;
    }

    public String getMailTo() {
        return mailTo;
    }
}
