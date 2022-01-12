package eu.europeana.api.recommend.updater.service.record;

import eu.europeana.api.recommend.updater.model.record.Record;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.stream.Stream;

/**
 * Spring Boot data Mongo repository for loading data from Mongo
 *
 * @author Patrick Ehlert
 */
@Repository
@EnableMongoRepositories
public interface MongoRecordRepository extends MongoRepository<Record, String> {

    /**
     * @return number of all records in the mongo database
     */
    Integer countAllBy();

    /**
     * @return cursor to all records in the database
     */
    Stream<Record> streamAllBy();

    /**
     * @return number of all records modified after the given date
     */
    Integer countAllByTimestampUpdatedAfter(Date date);;

    /**
     * @return cursor to all records modified after the given date
     */
    Stream<Record> streamByTimestampUpdatedAfter(Date date);
}
