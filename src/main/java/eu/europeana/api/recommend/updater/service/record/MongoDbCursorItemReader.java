package eu.europeana.api.recommend.updater.service.record;

import eu.europeana.api.recommend.updater.model.record.Record;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Spring Batch reader for reading CHO records from a Mongo database
 *
 * @author Patrick Ehlert
 */
public class MongoDbCursorItemReader extends AbstractItemCountingItemStreamItemReader<Record> {

    private static final Logger LOG = LogManager.getLogger(MongoDbCursorItemReader.class);

    @Autowired
    private MongoRecordRepository mongoRecordRepository;

    private Stream<Record> stream;
    private Iterator<Record> iterator;

    @Override
    protected void doOpen() {
        this.stream = mongoRecordRepository.streamAllBy();
        this.iterator = stream.iterator();
        LOG.debug("Opened stream to Mongo");
    }



    @Override
    protected void doClose() {
        if (stream != null) {
            stream.close();
        }
    }

    @Override
    protected Record doRead() {
        if (iterator.hasNext()) {
            return iterator.next();
        }

        LOG.debug("Finished reading from Mongo");
        return null;
    }

    // TODO implement better resume from error functionality

//    @Override
//    protected void jumpToItem(int itemIndex) {
//        iterable = iterable.skip(itemIndex);
//    }


}
