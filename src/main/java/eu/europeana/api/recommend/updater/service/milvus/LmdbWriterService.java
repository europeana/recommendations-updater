package eu.europeana.api.recommend.updater.service.milvus;


import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import eu.europeana.api.recommend.updater.exception.LmdbStateException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lmdbjava.*;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

    private static final Logger LOG = LogManager.getLogger(LmdbWriterService.class);

    private static final String ID2KEY = "_id2key";
    private static final String KEY2ID = "_key2id";

    // Lmdb database files always have the same name
    private static final String LMDB_FILE_NAME = "data.mdb";
    // Value copied from https://bitbucket.org/jhn-ngo/recsy-xx/src/master/src/recommenders/europeana/indexers/milvus_indexer.py#lines-97
    private static final long LMDB_MAX_SIZE = 50_000_000_000L;
    // Value copied from https://bitbucket.org/jhn-ngo/recsy-xx/src/master/src/recommenders/europeana/indexers/milvus_indexer.py#lines-126
    // It seems this is set because by default keys are limited to 511 bytes (see also https://lmdb.readthedocs.io/en/release/#storage-efficiency-limits)
    private static final int LMDB_MAX_KEY_SIZE = 510;

    private final UpdaterSettings settings;

    private Env<ByteBuffer> environmentId2Key;
    private Env<ByteBuffer> environmentKey2Id;
    private Dbi<ByteBuffer> id2KeyDbi; // handle to database
    private Dbi<ByteBuffer> key2IdDbi; // handle to 2nd database
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

        File id2KeyFile = new File(rootFolder + milvusCollection + ID2KEY, LMDB_FILE_NAME);
        environmentId2Key = initEnvironment(id2KeyFile, isDeleteDb);
        // TODO figure out if in recommendation engine a table name is set or not?
        id2KeyDbi = environmentId2Key.openDbi(milvusCollection + ID2KEY, DbiFlags.MDB_CREATE);

        File key2IdFile = new File(rootFolder + milvusCollection + KEY2ID, LMDB_FILE_NAME);
        environmentKey2Id = initEnvironment(key2IdFile, isDeleteDb);
        key2IdDbi = environmentKey2Id.openDbi(milvusCollection + KEY2ID, DbiFlags.MDB_CREATE);

        count = readCount(environmentId2Key, id2KeyDbi);
        if (count != readCount(environmentKey2Id, key2IdDbi)) {
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

    private Env<ByteBuffer> initEnvironment(File file, boolean deleteOldDate) {
        checkFolderExistsOrInit(file, deleteOldDate);

        LOG.info("Connecting to {} lmdb database...", file.getParentFile().getName());
        Env<ByteBuffer> result = Env.create()
                .setMapSize(LMDB_MAX_SIZE)
                .setMaxDbs(1)
                .open(file.getParentFile());

        List<byte[]> dbiNames = result.getDbiNames();
        List<String> dbis = new ArrayList<>(1); // we expect only 1 dbi
        for (byte[] dbiName : dbiNames) {
            dbis.add(new String(dbiName, Charset.defaultCharset()));
        }
        LOG.info("Found tables {}", dbis);
        return result;
    }

    private void checkFolderExistsOrInit(File file, boolean deleteOldData) {
        LOG.info("Checking local lmdb databases at {}", file.getAbsolutePath());
        boolean exists = file.exists();
        if (exists) {
            LOG.info("Found existing {} lmdb database", file.getParentFile().getName());
            if (deleteOldData) {
                LOG.info("Deleting data file");
                try {
                    Files.delete(Path.of(file.getAbsolutePath()));
                } catch (IOException e)  {
                    throw new LmdbStateException("Cannot delete lmdb file", e);
                }
                exists = false;
            }
        }

        if (!exists) {
            LOG.info("Creating new lmdb folder...");
            if (!file.getParentFile().mkdirs() && file.canWrite()) {
                throw new LmdbStateException("Cannot initialize database" + file.getParentFile().getName());
            }
        }
    }

    private long readCount(Env<ByteBuffer> env, Dbi<ByteBuffer> dbi) {
        try (Txn<ByteBuffer> transaction = env.txnRead()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Checking state of {}", new String(dbi.getName(), Charset.defaultCharset()));
            }
            Stat stat = dbi.stat(transaction);
            return stat.entries;
        }
    }

    public long writeId(String key) {
        if (count == -1L || environmentId2Key == null || environmentKey2Id == null) {
            throw new IllegalStateException("Run init method before doing a write!");
        }
        // Note that in https://bitbucket.org/jhn-ngo/recsy-xx/src/master/src/recommenders/europeana/indexers/milvus_indexer.py#lines-126
        // keys are encoded using the default Python encode() method (meaning UTF-8) and
        Long newId;
        synchronized (idLock) {
            count++;
            newId = count;
        }

        // For writing to lmdb we followed this tutorial
        // https://github.com/lmdbjava/lmdbjava/blob/master/src/test/java/org/lmdbjava/TutorialTest.java
        ByteBuffer keyValue = ByteBuffer.allocateDirect(LMDB_MAX_KEY_SIZE);
        keyValue.put(Arrays.copyOfRange(key.getBytes(StandardCharsets.UTF_8), 0, LMDB_MAX_KEY_SIZE)).flip();
        ByteBuffer idValue = ByteBuffer.allocateDirect(Long.BYTES).putLong(newId).flip();

        try (Txn<ByteBuffer> transaction1 = environmentId2Key.txnWrite()) {
            key2IdDbi.put(transaction1, keyValue, idValue);
            try (Txn<ByteBuffer> transaction2 = environmentKey2Id.txnWrite()) {
                id2KeyDbi.put(transaction2, idValue, keyValue);
                transaction2.commit();
            }
            transaction1.commit();
        }

        return newId;
    }

    @PreDestroy
    public void shutdown() {
        close(environmentId2Key);
        close(environmentKey2Id);
    }

    private void close(Env<ByteBuffer> env) {
        if (env != null && !env.isClosed()) {
            LOG.info("Closing connection to local lmdb database");
            env.close();
        }
    }
}
