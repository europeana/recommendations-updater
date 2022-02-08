package eu.europeana.api.recommend.updater.service.record;

import eu.europeana.api.recommend.updater.model.record.Record;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.stream.Stream;

/**
 * Spring Boot Data Mongo repository for loading data from Mongo (per set).
 * The quickest way to retrieve records per set is to use a 'starts-with' regex on the 'about' field
 * To guarantee a consistent order we order by about field.
 *
 * @author Patrick Ehlert
 */
@Repository
@EnableMongoRepositories
public interface MongoRecordRepository extends MongoRepository<Record, String> {

    String FIELDS = "{" +
            "aggregations:0," +
            "europeanaAggregation:0, " +
            "europeanaCollectionName:0, " +
            "europeanaCompleteness:0," +
            "licenses:0, " +
            "organizations:0," +
            "providedCHOs:0," +
            "qualityAnnotations:0," +
            "services:0," +
            "timestampCreated:0," +
            "type:0 }";

    /**
     * @return number of all records in the mongo database
     */
    Integer countAllBy();

    /**
     * Open a cursor that retrieves records that adhere to the provided regex on the about field
     *
     * @param setIdRegex regex filtering per set, should be in the form of "^/<setId>/"
     * @return cursor to all records in the database for the requested set.
     */
    @Query(fields = FIELDS)
    Stream<Record> streamAllByAboutRegexOrderByAbout(String setIdRegex);

    /**
     * Count the number of records modified after a particular date
     *
     * @param date filters records on lastModified date
     * @return number of all records modified after the given date
     */
    Integer countAllByTimestampUpdatedAfter(Date date);

    /**
     * Open a cursor that retrieves records that adhere to the provided regex on the about field and were last
     * modified after the provided date
     *
     * @param setIdRegex regex filtering per set, should be in the form of "^/<setId>/"
     * @param date filters records on lastModified date
     * @return cursor to all records modified after the given date
     */
    @Query(fields = FIELDS)
    Stream<Record> streamAllByAboutRegexAndTimestampUpdatedAfterOrderByAbout(String setIdRegex, Date date);
}
