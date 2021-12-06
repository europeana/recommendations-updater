package eu.europeana.api.recommend.updater.config;

import eu.europeana.api.recommend.updater.model.JobCompletionNotificationListener;
import eu.europeana.api.recommend.updater.model.embeddings.EmbeddingRecord;
import eu.europeana.api.recommend.updater.service.embeddings.EmbeddingRecordFileWriter;
import eu.europeana.api.recommend.updater.model.embeddings.RecordVectors;
import eu.europeana.api.recommend.updater.service.embeddings.RecordVectorsFileWriter;
import eu.europeana.api.recommend.updater.model.record.Record;
import eu.europeana.api.recommend.updater.service.embeddings.EmbedRecordToVectorProcessor;
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
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

/**
 * Configuration of update process
 * <ol>
 * <li>Read record data from MongoDb</li>
 * <li>Pick relevant data from record to construct EmbeddingRecord</li>
 * <li>If Embedding API is defined then we'll send batches of EmbeddingRecord to Embedding API and receive back vectors. </li>
 * If no Embedding API is defined we'll write EmbeddingRecord data to file (for testing purposes)</li>
 * <li>If Milvus instance is defined, then we try to save vectors in Milvus
 * If no Milvus instance is define we'll write vectors to file (for testing purposes)</li>
 * </ol>
 *
 * @author Patrick Ehlert
 */
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    private static final Logger LOG = LogManager.getLogger(BatchConfiguration.class);

    private final UpdaterSettings settings;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    // First processing part; we load records from Mongo and generate EmbeddingRecords
    private final RecordToEmbedRecordProcessor recordToEmbedRecordProcessor;
    // Second processing part; we send EmbeddingRecords to Embedding API and receive back vectors which as put in
    // RecordVector objects
    private final EmbedRecordToVectorProcessor embedRecordToVectorProcessor;

    public BatchConfiguration(UpdaterSettings settings,
                              JobBuilderFactory jobBuilderFactory,
                              StepBuilderFactory stepBuilderFactory,
                              RecordToEmbedRecordProcessor recordToEmbedRecordProcessor,
                              EmbedRecordToVectorProcessor embedRecordToVectorProcessor) {
        this.settings = settings;
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.recordToEmbedRecordProcessor = recordToEmbedRecordProcessor;
        this.embedRecordToVectorProcessor = embedRecordToVectorProcessor;
    }

    /**
     * Read records from Mongo database
     * @return Spring Batch ItemReader
     */
    @Bean
    public ItemReader<Record> recordReader() {
        MongoDbCursorItemReader itemReader = new MongoDbCursorItemReader();
        itemReader.setName("Mongo Record Reader");
        return itemReader;
    }

    /**
     * This combines the first and second processing part
     * @return ItemProcessor
     */
    @Bean
    ItemProcessor<Record, RecordVectors> loadRecordGenerateVectorsProcessor() {
        CompositeItemProcessor<Record, RecordVectors> compositeProcessor = new CompositeItemProcessor<>();
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
    public FlatFileItemWriter<EmbeddingRecord> embeddingRecordWriter() {
        return new EmbeddingRecordFileWriter(settings.getTestFile()).build();
    }

    /**
     * Writes RecordVectors to file (for testing)
     * @return Spring Batch FlatFileItemWriter
     */
    @Bean
    public FlatFileItemWriter<RecordVectors> recordVectorsWriter() {
        return new RecordVectorsFileWriter(settings.getTestFile()).build();
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
                    .<Record, RecordVectors>chunk(settings.getBatchSize())
                    .reader(recordReader())
                    .processor(loadRecordGenerateVectorsProcessor())
                    .writer(recordVectorsWriter())
                    .build();
        }

        LOG.info("No Embeddings API and Milvus configured, so saving EmbeddingRecords to file {}", settings.getTestFile());
        return stepBuilderFactory.get("step1")
                .<Record, EmbeddingRecord>chunk(settings.getBatchSize())
                .reader(recordReader())
                .processor(recordToEmbedRecordProcessor)
                .writer(embeddingRecordWriter())
                .build();
    }

    @Bean
    public Job updateJob(JobCompletionNotificationListener listener, Step step1, Step step2) {
        return jobBuilderFactory.get("Update Job")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .end()
                .build();
    }
}
