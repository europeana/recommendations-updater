package eu.europeana.api.recommend.updater.service.milvus;


import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import eu.europeana.api.recommend.updater.exception.LmdbStateException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service that creates new lmdb databases or appends to an existing ones.
 *
 * Since the current Milvus version doesn't support string ids, we write 2 mappings to a local lmdb database. We mimic
 * here the approach used by the recommendation engine which is to create 2 folders:
 * <ol>
 *    <li>&ltmilvusCollectionName&gt_key2id</li>
 *    <li>&ltmilvusCollectionName&gt_id2key</li>
 * </ol>
 *
 * @author Patrick Ehlert
 */
@Service
public class LmdbWriterService {

    public static final String ID2KEY = "_id2key";
    public static final String KEY2ID = "_key2id";

    private static final Logger LOG = LogManager.getLogger(LmdbWriterService.class);

    private final UpdaterSettings settings;

    private Lmdb id2keyDb;
    private Lmdb key2idDb;
    private IdGenerator idGenerator;

    /**
     * Create new service for writing data to Lmdb
     * @param settings auto-wired updater settings
     */
    public LmdbWriterService(UpdaterSettings settings) {
        this.settings = settings;
    }

    /**
     * Checks if the lmdb database is empty when doing a full update, or if there's a non-empty database when
     * doing a partial update. Also deletes any old date if requested
     * @param isFullUpdate if true, then lmdb database should be empty
     * @param isDeleteDb if true, then existing data is deleted first
     * @return the number of items in the lmdb database
     * @throws RuntimeException if the state is different than expected
     */
    // user-provided file path is intentional
    // also singleton class writing to instance variables is intentional
    @SuppressWarnings({"findsecbugs:PATH_TRAVERSAL_IN", "fb-contrib:USFW_UNSYNCHRONIZED_SINGLETON_FIELD_WRITES"})
    public long init(boolean isFullUpdate, boolean isDeleteDb) {
        String milvusCollection = settings.getMilvusCollection();
        String rootFolder = settings.getLmdbFolder();

        this.id2keyDb = new Lmdb(new File(rootFolder + milvusCollection + ID2KEY), false);
        id2keyDb.connect(null, isDeleteDb); // connect to/create unnamed database
        this.key2idDb = new Lmdb(new File(rootFolder + milvusCollection + KEY2ID), false);
        key2idDb.connect(null, isDeleteDb); // connect to/create unnamed database

        long count = id2keyDb.getItemCount();
        if (count != key2idDb.getItemCount()) {
            throw new LmdbStateException("Aborting update because the 2 lmdb tables are not equal in size");
        }

        if (isFullUpdate && count > 0) {
            throw new LmdbStateException("Aborting full update because lmdb database exists and is not empty");
        }
        if (!isFullUpdate && count == 0) {
            LOG.warn("Found empty lmdb databases. Are you sure you want to do a partial update?");
        }

        LOG.info("Lmdb database contains {} entries (per table)", count);

        if (count == 0) {
            idGenerator = new IdGenerator(-1); // we start
        } else {
            // verify if id2keyDb indeed has expected last known used id
            String shouldBePresent = id2keyDb.readString(String.valueOf(count - 1));
            if (StringUtils.isBlank(shouldBePresent)) {
                throw new LmdbStateException("Aborting update because last known value is empty or null (Key " +
                        (count - 1) + " has value "+ shouldBePresent);
            }
            // and verify that next value does not yet exist
            String shouldBeNull = id2keyDb.readString(String.valueOf(count));
            if (shouldBeNull != null) {
                throw new LmdbStateException("Aborting update because value after last known value should not be present (Key " +
                        (count) + " has value "+ shouldBeNull);
            }
            idGenerator = new IdGenerator(count - 1);
        }

        return count;
    }


    /**
     * Generate multiple newIds for all provided keys and write them to the 2 databases
     * @param keys list of RecordIds
     * @return list of generated ids
     */
    public List<Long> writeIds(List<String> keys) {
        if (idGenerator == null || key2idDb == null || id2keyDb == null) {
            throw new IllegalStateException("Run init method before doing a write!");
        }

        List<Long> newIds = idGenerator.getNewIds(keys.size());
        List<String> newIdsString = newIds.stream().map(Object::toString).collect(Collectors.toUnmodifiableList());
        id2keyDb.write(newIdsString, keys);
        key2idDb.write(keys, newIdsString);
        return newIds;
    }

    /**
     * Close the lmdb databases
     */
    @PreDestroy
    public void shutdown() {
        if (id2keyDb != null) {
            id2keyDb.close();
        }
        if (key2idDb != null) {
            key2idDb.close();
        }
    }

    private static final class IdGenerator {

        private final Object lock = new Object();
        private Long id;

        public IdGenerator(long lastKnownId) {
            id = lastKnownId;
        }

        /**
         * Get a list of new ids.
         * @param amount the number of ids requests
         * @return a list of ids that can be used for writing to LMDB and Milvus
         */
        public List<Long> getNewIds(int amount) {
            List<Long> result = new ArrayList<>(amount);
            synchronized (lock) {
                for (int i = 0; i < amount; i++) {
                    id++;
                    result.add(id);
                }
            }
            return result;
        }

    }

}
