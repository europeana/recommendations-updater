package eu.europeana.api.recommend.updater.service.milvus;


import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import eu.europeana.api.recommend.updater.exception.LmdbStateException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.File;

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
    private final Object idLock = new Object(); // lock to prevent concurrency issues
    private long count = -1L; // saved as recordId number to lmdb

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
        // TODO figure out if in recommendation engine a table name is set or not?
        id2keyDb.connect(milvusCollection + ID2KEY);
        this.key2idDb = new Lmdb(new File(rootFolder + milvusCollection + KEY2ID), false);
        id2keyDb.connect(milvusCollection + ID2KEY);

        count = id2keyDb.getItemCount();
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
        return count;
    }


    public long writeId(String key) {
        if (count == -1L || key2idDb == null || id2keyDb == null) {
            throw new IllegalStateException("Run init method before doing a write!");
        }
        Long newId;
        synchronized (idLock) {
            count++;
            newId = count;
        }

        // TODO try if writing in bulk is faster or not
        id2keyDb.write(newId, key);
        key2idDb.write(key, newId);
        return newId;
    }

    @PreDestroy
    public void shutdown() {
        id2keyDb.close();
        key2idDb.close();
    }

}
