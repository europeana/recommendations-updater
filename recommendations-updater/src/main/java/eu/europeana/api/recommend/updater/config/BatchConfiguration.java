package eu.europeana.api.recommend.updater.config;

import eu.europeana.api.recommend.common.model.EmbeddingRecord;
import eu.europeana.api.recommend.common.model.RecordVectors;
import eu.europeana.api.recommend.updater.model.record.Record;
import eu.europeana.api.recommend.updater.service.MailService;
import eu.europeana.api.recommend.updater.service.embeddings.EmbedRecordToVectorProcessor;
import eu.europeana.api.recommend.updater.service.embeddings.EmbeddingRecordFileWriter;
import eu.europeana.api.recommend.updater.service.embeddings.RecordVectorsFileWriter;
import eu.europeana.api.recommend.updater.service.milvus.MilvusWriterService;
import eu.europeana.api.recommend.updater.service.record.MongoDbItemReader;
import eu.europeana.api.recommend.updater.service.record.RecordToEmbedRecordProcessor;
import eu.europeana.api.recommend.updater.service.record.SolrSetReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.List;

/**
 * Configuration of the update process
 * <ol>
 * <li>Read record data from MongoDb</li>
 * <li>Pick relevant data from record to construct EmbeddingRecord</li>
 * <li>If Embedding API is defined then we'll send batches of EmbeddingRecord to Embedding API and receive back vectors.
 * If no Embedding API is defined we'll write EmbeddingRecord data to file (for testing purposes)</li>
 * <li>If Milvus instance is defined, then we try to save vectors in Milvus.
 * If no Milvus instance is defined we'll write vectors to file (for testing purposes)</li>
 * </ol>
 * Since we want to sent multiple records in 1 request to Embedding API we process a group (list) of records. The size
 * is specified in the batch size property
 *
 * @author Patrick Ehlert
 */
@Configuration
@EnableBatchProcessing
public class BatchConfiguration extends DefaultBatchConfigurer {

    private static final Logger LOG = LogManager.getLogger(BatchConfiguration.class);

    private static final int MAX_THREADS = 50;

    private final UpdaterSettings settings;
    private final MongoDbItemReader recordReader;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final TaskExecutor taskExecutor;

    // Step 1. Query solr for all sets that we need to process
    private final SolrSetReader solrSetReader;
    // Step 2.1. Load records from Mongo per set and 2.2 generate EmbeddingRecords
    private final RecordToEmbedRecordProcessor recordToEmbedRecordProcessor;
    // Step 2.3. Send EmbeddingRecords to Embedding API and receive back vectors (RecordVector objects)
    private final EmbedRecordToVectorProcessor embedRecordToVectorProcessor;
    // Step 2.4. Write RecordVectors to Milvus
    private final MilvusWriterService milvusWriterService;
    // Last step send update results via email
    private final MailService mailService;

    @SuppressWarnings("java:S107") // we want dependency injection, so need to have this many constructor parameters
    public BatchConfiguration(UpdaterSettings settings,
                              JobBuilderFactory jobBuilderFactory,
                              StepBuilderFactory stepBuilderFactory,
                              SolrSetReader solrSetReader,
                              MongoDbItemReader recordReader,
                              RecordToEmbedRecordProcessor recordToEmbedRecordProcessor,
                              EmbedRecordToVectorProcessor embedRecordToVectorProcessor,
                              MilvusWriterService milvusWriterService,
                              MailService mailService) {
        this.settings = settings;
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.solrSetReader = solrSetReader;
        this.recordReader = recordReader;
        recordReader.setName("Mongo record reader");
        recordReader.setSaveState(false); // mongo reader is not fault tolerant
        this.recordToEmbedRecordProcessor = recordToEmbedRecordProcessor;
        this.embedRecordToVectorProcessor = embedRecordToVectorProcessor;
        this.milvusWriterService = milvusWriterService;
        this.mailService = mailService;

        SimpleAsyncTaskExecutor simpleTaskExecutor = new SimpleAsyncTaskExecutor();
        simpleTaskExecutor.setConcurrencyLimit(settings.getThreads());
        simpleTaskExecutor.setThreadNamePrefix("Chunk");
        this.taskExecutor = simpleTaskExecutor;
    }

    /**
     *  Set-up in-memory Spring Batch data source
     * @param dataSource
     */
    @Override
    public void setDataSource(DataSource dataSource) {
        // at the moment we do not store spring-batch process data in a database, so no recovery is possible
    }

    /**
     * This combines the first and second processing part
     * @return ItemProcessor
     */
    @Bean
    ItemProcessor<List<Record>, List<RecordVectors>> loadRecordGenerateVectorsProcessor() {
        CompositeItemProcessor<List<Record>, List<RecordVectors>> compositeProcessor = new CompositeItemProcessor<>();
        compositeProcessor.setDelegates(Arrays.asList(
                recordToEmbedRecordProcessor,
                embedRecordToVectorProcessor));
        return compositeProcessor;
    }

    /**
     * Writes EmbeddingRecords to file (for testing)
     * @return Spring Batch FlatFileItemWriter
     */
    @Bean
    public FlatFileItemWriter<List<EmbeddingRecord>> embeddingRecordWriter() {
        return new EmbeddingRecordFileWriter(settings.getTestFile(), settings.getBatchSize()).build();
    }

    /**
     * Writes RecordVectors to file (for testing)
     * @return Spring Batch FlatFileItemWriter
     */
    @Bean
    public FlatFileItemWriter<List<RecordVectors>> recordVectorsWriter() {
        return new RecordVectorsFileWriter(settings.getTestFile(), settings.getBatchSize()).build();
    }

    /**
     * Step1: Read all sets from Solr (or skip this if we have sets provided on the command-line)
     * @return
     */
    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .tasklet(this.solrSetReader)
                .build();
    }

    /**
     * Step2: do actual processing per set
     * @return
     */
    @Bean
    public Step step2() {
        if (UpdaterSettings.isValueDefined(settings.getEmbeddingApiUrl())
                && UpdaterSettings.isValueDefined(settings.getMilvusCollection())
                && UpdaterSettings.isValueDefined(settings.getMilvusUrl())) {
            LOG.info("Embeddings API and Milvus are configured. Saving vectors to Milvus collection {} ", settings.getMilvusCollection());
            return stepBuilderFactory.get("step2")
                    .<List<Record>, List<RecordVectors>>chunk(1)// chunksize=1 because we want to write to Embeddings API 1 list of <batchsize> records
                    .reader(this.recordReader)
                    .processor(loadRecordGenerateVectorsProcessor())
                    .writer(milvusWriterService)
                    .taskExecutor(taskExecutor)
                    .throttleLimit(MAX_THREADS)
                    .build();

        } else if (UpdaterSettings.isValueDefined(settings.getEmbeddingApiUrl())) {
            LOG.info("Embeddings API configured but no Milvus, so saving RecordVectors to file {}", settings.getTestFile());
            return stepBuilderFactory.get("step2")
                    .<List<Record>, List<RecordVectors>>chunk(1)
                    .reader(this.recordReader)
                    .processor(loadRecordGenerateVectorsProcessor())
                    .writer(recordVectorsWriter())
                    .taskExecutor(taskExecutor)
                    .throttleLimit(MAX_THREADS)
                    .build();
        }

        LOG.info("No Embeddings API and Milvus configured, so saving EmbeddingRecords to file {}", settings.getTestFile());
        return stepBuilderFactory.get("step2")
                .<List<Record>, List<EmbeddingRecord>>chunk(1)
                .reader(this.recordReader)
                .processor(recordToEmbedRecordProcessor)
                .writer(embeddingRecordWriter())
                .taskExecutor(taskExecutor)
                .throttleLimit(MAX_THREADS)
                .build();
    }

    /**
     * Basic Spring Batch update flow
     * @param step1
     * @param step2
     * @return
     */
    @Bean
    public Job updateJob(Step step1, Step step2) {
        return jobBuilderFactory.get("updateJob")
                .incrementer(new RunIdIncrementer())
                .listener(mailService)
                .flow(step1)
                .next(step2)
                .end()
                .listener(recordReader)
                .listener(milvusWriterService)
                .build();
    }
}
