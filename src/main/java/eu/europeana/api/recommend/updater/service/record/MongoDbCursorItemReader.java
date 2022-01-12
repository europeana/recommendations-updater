package eu.europeana.api.recommend.updater.service.record;

import eu.europeana.api.recommend.updater.config.JobCmdLineStarter;
import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import eu.europeana.api.recommend.updater.model.record.Record;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.stream.Stream;

/**
 * Spring Batch reader for reading CHO records from a Mongo database
 *
 * @author Patrick Ehlert
 */
@Component
@StepScope
public class MongoDbCursorItemReader extends AbstractItemCountingItemStreamItemReader<List<Record>> {

    private static final Logger LOG = LogManager.getLogger(MongoDbCursorItemReader.class);

    private final UpdaterSettings settings;
    private final MongoRecordRepository mongoRecordRepository;

    private Stream<Record> stream;
    private Iterator<Record> iterator;
    private Boolean isFullUpdate;
    private Date fromDate;

    public MongoDbCursorItemReader(UpdaterSettings settings, MongoRecordRepository mongoRecordRepository) {
        this.settings = settings;
        this.mongoRecordRepository = mongoRecordRepository;
    }

    @Value("#{jobParameters['updateType']}")
    public void setUpdateType (final String updateType) {
        this.setName("Mongo record reader");
        isFullUpdate = JobCmdLineStarter.PARAM_UPDATE_FULL.equalsIgnoreCase(updateType);
    }

    @Value("#{jobParameters['from']}")
    public void setFromDate (final Date fromDate) {
        this.fromDate = fromDate;
    }

    @Override
    protected void doOpen() {
        if (this.isFullUpdate) {
            if (LOG.isInfoEnabled()) {
                LOG.info("{} records available", mongoRecordRepository.countAllBy());
            }
            this.stream = mongoRecordRepository.streamAllBy();
        } else {
            if (LOG.isInfoEnabled()) {
                LOG.info("{} modified records available", mongoRecordRepository.countAllByTimestampUpdatedAfter(this.fromDate));
            }
            this.stream = mongoRecordRepository.streamByTimestampUpdatedAfter(this.fromDate);
        }
        this.iterator = stream.iterator();
        LOG.debug("Opened stream to MongoDb");
    }

    @PreDestroy
    @Override
    protected void doClose() {
        if (stream != null) {
            LOG.info("Closing mongoDb stream");
            stream.close();
        }
    }

    @Override
    @SuppressWarnings("java:S1168") // Spring-Batch requires us to return null when we're done
    protected List<Record> doRead() {
        List<Record> result = new ArrayList<>(settings.getBatchSize());

   //     synchronized (iterator) {
            while (iterator.hasNext() && result.size() < settings.getBatchSize()) {
                result.add(iterator.next());
            }
     //   }

        if (!result.isEmpty()) {
            LOG.trace("Retrieved {} items from Mongo", result.size());
            return result;
        }
        LOG.info("Finished reading records from Mongo");
        return null;
    }

    // TODO implement better resume from error functionality

//    @Override
//    protected void jumpToItem(int itemIndex) {
//        iterable = iterable.skip(itemIndex);
//    }


}
