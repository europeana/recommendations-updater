package eu.europeana.api.recommend.updater.config;

import eu.europeana.api.recommend.updater.model.embeddings.EmbeddingRecord;
import eu.europeana.api.recommend.updater.model.embeddings.RecordVectors;
import eu.europeana.api.recommend.updater.model.record.Record;
import eu.europeana.api.recommend.updater.service.TaskExecutor;
import eu.europeana.api.recommend.updater.service.embeddings.EmbedRecordToVectorProcessor;
import eu.europeana.api.recommend.updater.service.embeddings.EmbeddingRecordFileWriter;
import eu.europeana.api.recommend.updater.service.embeddings.RecordVectorsFileWriter;
import eu.europeana.api.recommend.updater.service.record.MongoDbCursorItemReader;
import eu.europeana.api.recommend.updater.service.record.RecordToEmbedRecordProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
public class BatchConfiguration {

    private static final Logger LOG = LogManager.getLogger(BatchConfiguration.class);

    private final UpdaterSettings settings;
    private final MongoDbCursorItemReader recordReader;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final TaskExecutor taskExecutor;

    // First processing part; we load records from Mongo and generate EmbeddingRecords
    private final RecordToEmbedRecordProcessor recordToEmbedRecordProcessor;
    // Second processing part; we send EmbeddingRecords to Embedding API and receive back vectors which as put in
    // RecordVector objects
    private final EmbedRecordToVectorProcessor embedRecordToVectorProcessor;

    public BatchConfiguration(UpdaterSettings settings,
                              JobBuilderFactory jobBuilderFactory,
                              StepBuilderFactory stepBuilderFactory,
                              TaskExecutor taskExecutor,
                              MongoDbCursorItemReader recordReader,
                              RecordToEmbedRecordProcessor recordToEmbedRecordProcessor,
                              EmbedRecordToVectorProcessor embedRecordToVectorProcessor) {
        this.settings = settings;
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.taskExecutor = taskExecutor;
        this.recordReader = recordReader;
        this.recordToEmbedRecordProcessor = recordToEmbedRecordProcessor;
        this.embedRecordToVectorProcessor = embedRecordToVectorProcessor;
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

    // TODO create MilvusWriter

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

    @Bean
    public Step step1() {
        if (UpdaterSettings.isValueDefined(settings.getEmbeddingsApiUrl()) &&
                UpdaterSettings.isValueDefined(settings.getMilvusCollection())) {
            LOG.info("Embeddings API and Milvus are configured. Saving vectors to Milvus collection {} ", settings.getMilvusCollection());
            // TODO implement
            return null;

        } else if (UpdaterSettings.isValueDefined(settings.getEmbeddingsApiUrl())) {
            LOG.info("Embeddings API configured but no Milvus, so saving RecordVectors to file {}", settings.getTestFile());
            return stepBuilderFactory.get("step1")
                    .<List<Record>, List<RecordVectors>>chunk(1)
                    .reader(this.recordReader)
                    .processor(loadRecordGenerateVectorsProcessor())
                    .writer(recordVectorsWriter())
                //    .taskExecutor(taskExecutor)
                    .build();
        }

        LOG.info("No Embeddings API and Milvus configured, so saving EmbeddingRecords to file {}", settings.getTestFile());
        return stepBuilderFactory.get("step1")
                .<List<Record>, List<EmbeddingRecord>>chunk(1)
                .reader(this.recordReader)
                .processor(recordToEmbedRecordProcessor)
                .writer(embeddingRecordWriter())
               // .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    public Job updateJob(Step step1) {
        return jobBuilderFactory.get("updateJob")
                .incrementer(new RunIdIncrementer())
                .flow(step1)
                .end()
                .build();
    }
}
