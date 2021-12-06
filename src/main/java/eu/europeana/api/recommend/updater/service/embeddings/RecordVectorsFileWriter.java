package eu.europeana.api.recommend.updater.service.embeddings;

import eu.europeana.api.recommend.updater.model.embeddings.RecordVectors;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.core.io.FileSystemResource;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Writes RecordVectors objects to csv file
 * For testing purposes.
 *
 * @author Patrick Ehlert
 */
public class RecordVectorsFileWriter extends FlatFileItemWriterBuilder<RecordVectors> {

    private static final String[] FIELDS_TO_WRITE = new String[]{
            "id",
            "embedding"
    };
    private static final String DELIMITER = ";";

    private final String fileName;

    /**
     * Create a new Spring Batch writer that writes RecordVectors to csv file
     * @param fileName name of the file to write to
     */
    public RecordVectorsFileWriter(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public FlatFileItemWriter<RecordVectors> build() {
        this.name("Record Vectors Writer")
                .headerCallback(createHeaderCallBack())
                .lineAggregator(createLineAggregator())
                .resource(new FileSystemResource(fileName))
                .shouldDeleteIfExists(true);
        return super.build();
    }

    private FlatFileHeaderCallback createHeaderCallBack() {
        return writer -> writer.write(String.join(DELIMITER, FIELDS_TO_WRITE));
    }

    private LineAggregator<RecordVectors> createLineAggregator() {
        return recordVectors -> recordVectors.getId() + DELIMITER + arrayToString(recordVectors.getEmbedding());
    }

    private String arrayToString(Double[] array) {
        return Arrays.stream(array)
                .map(String::valueOf)
                .collect(Collectors.joining(",", "", ""));
    }
}
