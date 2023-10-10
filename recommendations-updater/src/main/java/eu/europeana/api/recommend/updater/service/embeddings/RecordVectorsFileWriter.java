package eu.europeana.api.recommend.updater.service.embeddings;

import eu.europeana.api.recommend.common.model.RecordVectors;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.core.io.FileSystemResource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Writes RecordVectors objects to csv file
 * For testing purposes.
 *
 * @author Patrick Ehlert
 */
public class RecordVectorsFileWriter extends FlatFileItemWriterBuilder<List<RecordVectors>> {

    private static final String[] FIELDS_TO_WRITE = new String[]{
            "id",
            "embedding"
    };
    private static final char DELIMITER = ';';

    private final String fileName;
    private final int batchSize;

    /**
     * Create a new Spring Batch writer that writes RecordVectors to csv file
     * @param fileName name of the file to write to
     * @param batchSize number of items per list sent to the writer
     */
    public RecordVectorsFileWriter(String fileName, int batchSize) {
        this.fileName = fileName;
        this.batchSize = batchSize;
    }

    @Override
    public FlatFileItemWriter<List<RecordVectors>> build() {
        this.name("Record Vectors Writer")
                .headerCallback(createHeaderCallBack())
                .lineAggregator(createLineAggregator())
                .resource(new FileSystemResource(fileName))
                .shouldDeleteIfExists(true);
        return super.build();
    }

    private FlatFileHeaderCallback createHeaderCallBack() {
        return writer -> writer.write(String.join(String.valueOf(DELIMITER), FIELDS_TO_WRITE));
    }

    private LineAggregator<List<RecordVectors>> createLineAggregator() {
        return list -> {
            StringBuilder s = new StringBuilder(500 * batchSize); // rough estimate of average size
            for (RecordVectors recordVectors : list) {
                s.append(recordVectors.getId()).append(DELIMITER)
                        .append(arrayToString(recordVectors.getEmbedding()))
                        .append('\n');
            }
            // remove last newline
            s.deleteCharAt(s.length() - 1);
            return s.toString();
        };
    }

    private String arrayToString(Double[] array) {
        return Arrays.stream(array)
                .map(String::valueOf)
                .collect(Collectors.joining(",", "", ""));
    }
}
