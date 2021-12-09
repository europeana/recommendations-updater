package eu.europeana.api.recommend.updater.service.embeddings;

import eu.europeana.api.recommend.updater.model.embeddings.EmbeddingRecord;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.core.io.FileSystemResource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Writes EmbeddingRecord objects to csv file
 * For testing purposes.
 * TODO known issue: any delimiter or new lines in the data is not escaped, so this can mess things up
 *
 * @author Patrick Ehlert
 */
public class EmbeddingRecordFileWriter extends FlatFileItemWriterBuilder<List<EmbeddingRecord>> {

    private static final String[] FIELDS_TO_WRITE = new String[]{
            "id",
            "title",
            "description",
            "creator",
            "tags",
            "places",
            "times"
    };
    private static final String DELIMITER = ";";

    private final String fileName;
    private final int batchSize;

    /**
     * Create a new Spring Batch writer that writes EmbeddingRecords to csv file
     * @param fileName name of the file to write to
     * @param batchSize number of items per list sent to the writer
     */
    public EmbeddingRecordFileWriter(String fileName, int batchSize) {
        this.fileName = fileName;
        this.batchSize = batchSize;
    }

    @Override
    public FlatFileItemWriter<List<EmbeddingRecord>> build() {
        this.name("Embedding Record Writer")
                .headerCallback(createHeaderCallBack())
                .lineAggregator(createLineAggregator())
                .resource(new FileSystemResource(fileName))
                .shouldDeleteIfExists(true);
        return super.build();
    }

    private FlatFileHeaderCallback createHeaderCallBack() {
        return writer -> writer.write(String.join(DELIMITER, FIELDS_TO_WRITE));
    }

    @SuppressWarnings("java:S109")
    private LineAggregator<List<EmbeddingRecord>> createLineAggregator() {
        return embedRecords -> {
            StringBuilder s = new StringBuilder(200 * batchSize); // rough estimate of average size
            for (EmbeddingRecord embeddingRecord : embedRecords) {
                s.append(embeddingRecord.getId()).append(DELIMITER)
                        .append(arrayToString(embeddingRecord.getTitle())).append(DELIMITER)
                        .append(arrayToString(embeddingRecord.getDescription())).append(DELIMITER)
                        .append(arrayToString(embeddingRecord.getCreator())).append(DELIMITER)
                        .append(arrayToString(embeddingRecord.getTags())).append(DELIMITER)
                        .append(arrayToString(embeddingRecord.getPlaces())).append(DELIMITER)
                        .append(arrayToString(embeddingRecord.getTimes()))
                        .append("\n");
            }
            // remove last newline
            s.deleteCharAt(s.length() - 1);
            return s.toString();
        };
    }

    private String arrayToString(String[] array) {
        return Arrays.stream(array)
                .map(String::valueOf)
                .collect(Collectors.joining(",", "", ""));
    }

}
