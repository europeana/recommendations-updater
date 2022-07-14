package eu.europeana.api.recommend.updater.service.milvus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.lmdbjava.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This is a special test class that we use to check what is in a provided LMDB database. The goal is to verify if the
 * database contains valid data we can read (see also ticket https://europeana.atlassian.net/browse/EA-2935).
 * This means that this test is usually disabled and we only start it manually when necessary.
 *
 * @author Patrick Ehlert
 */
@Disabled("Run this manually if you want to check contents of a particular lmdb file")
public class CheckLmdbContents {

    private static final Logger LOG = LogManager.getLogger(CheckLmdbContents.class);

    // folder name containing the 2 recommendation lmdb databases, without the _id2key and _key2id suffix
    private static final String DB_FOLDER = "./test";
    private static final String DB_NAME = null; // Pangeanic uses unnamed databases!

    @Test
    public void testId2KeyDatabase() {
        Lmdb id2key = new Lmdb(new File(DB_FOLDER  + LmdbWriterService.ID2KEY), true);
        assertTrue(id2key.connect(DB_NAME, false));

        long count = id2key.getItemCount();
        LOG.info("Database has {} items", count);
        assertNotEquals(0, count);

        LOG.info("Retrieving items with id 0 to 10...");
        for (int i = 0; i <= 10; i++) {
            LOG.info("key {}, value {}", i, id2key.readString(String.valueOf(i)));
        }

        LOG.info("Retrieve last items...");
        LOG.info("key {}, value {}",count - 1, id2key.readString(String.valueOf(count - 1)));
        LOG.info("key {}, value {}",count, id2key.readString(String.valueOf(count)));

        id2key.close();
    }

    /**\
     * At the moment this test only works for our own generated lmdb file where everything is stored in 1 database
     */
    @Test
    public void testKey2IdNamedDatabase() {
        File dbFile = new File(DB_FOLDER  + LmdbWriterService.ID2KEY);
        Lmdb id2key = new Lmdb(dbFile, true);
        assertTrue(id2key.connect(DB_NAME, false));

        long count = id2key.getItemCount();
        LOG.info("Database has {} items", count);
        assertNotEquals(0, count);

        id2key.close();

        // we open a new connection ourselves so we can iterate over the first 10 keys
        LOG.info("Reopening connection manually...");
        Env<ByteBuffer> env = Env.create()
                .setMapSize(Lmdb.MAX_DB_SIZE)
                .setMaxDbs(1)
                .open(dbFile, EnvFlags.MDB_RDONLY_ENV);
        Dbi<ByteBuffer> dbi = env.openDbi(DB_NAME);

        LOG.info("Iterating over first 10 items...");
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            CursorIterable<ByteBuffer> it = dbi.iterate(txn);
            Iterator<CursorIterable.KeyVal<ByteBuffer>> itKey = it.iterator();
            int i = 0;
            while (i < 10 && itKey.hasNext()) {
                CursorIterable.KeyVal<ByteBuffer> key = itKey.next();
                LOG.info("Key {}, value {}", Lmdb.byteBufferToString(key.key()), Lmdb.byteBufferToString(key.val()));
                i++;
            }
        }
        dbi.close();
        env.close();
    }

    /**
     * This test can be used to retrieve the LMDB id for a particular record
     */
    @Test
    public void testIdForRecord() {
        File dbFile = new File(DB_FOLDER  + LmdbWriterService.KEY2ID);
        Lmdb key2id = new Lmdb(dbFile, true);
        assertTrue(key2id.connect(DB_NAME, false));

        String recordId = "11651/_Botany_L_2812640";
        String lmdbId = key2id.readString(recordId);
        LOG.info("RecordId = /{} -> lmdb id = {}", recordId, lmdbId);
    }

}
