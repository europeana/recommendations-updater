package eu.europeana.api.recommend.updater.service.milvus;

import eu.europeana.api.recommend.updater.exception.LmdbStateException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lmdbjava.*;

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
 * Wrapper around Lmdb API so we can more easily use it for our purposes
 * Most functionality based on tutorial at https://github.com/lmdbjava/lmdbjava/blob/master/src/test/java/org/lmdbjava/TutorialTest.java
 *
 * @author Patrick Ehlert
 */
public class Lmdb {

    // Lmdb database files always have the same name
    public static final String FILE_NAME = "data.mdb";

    // For some reason in Milvus they used max_key_size - 1 instead of max_key_size (511)
    // see https://bitbucket.org/jhn-ngo/recsy-xx/src/master/src/recommenders/europeana/indexers/milvus_indexer.py#lines-126
    public static final int MAX_KEY_SIZE = 510;

    // In https://bitbucket.org/jhn-ngo/recsy-xx/src/master/src/recommenders/europeana/indexers/milvus_indexer.py#lines-126
    // keys are encoded using the default Python encode() method (meaning UTF-8)
    public static final Charset CHARSET = StandardCharsets.UTF_8;

    // Value copied from https://bitbucket.org/jhn-ngo/recsy-xx/src/master/src/recommenders/europeana/indexers/milvus_indexer.py#lines-97
    protected static final long MAX_DB_SIZE = 50_000_000_000L;

    private static final Logger LOG = LogManager.getLogger(Lmdb.class);

    private File dbFolder;
    private boolean readOnly;
    private Env<ByteBuffer> env;
    private Dbi<ByteBuffer> dbi;

    /**
     * Create a new lmdb database in the provided folder. Database files always have the name 'data.mdb'.
     * @param dbFolder folder where database file is/will be stored
     * @param readOnly indicate whether the database should be readonly or not
     */
    public Lmdb(File dbFolder, boolean readOnly) {
        this.dbFolder = dbFolder;
        this.readOnly = readOnly;
    }

    /**
     * Connect to an existing or new lmdb database
     * @param dbName the database to which to connect, will be created if it doesn't exist.
     * @return true if connection was setup successful, otherwise false
     */
    public boolean connect(String dbName){
        return connect(dbName, false);
    }

    /**
     * Connect to an existing or new lmdb database
     * @param dbName the database to which to connect, will be created if it doesn't exist.
     * @param deleteExistingDbs deletes the database file entirely before connecting
     * @return true if connection was setup successful, otherwise false
     */
    public boolean connect(String dbName, boolean deleteExistingDbs) {
        checkDbFileExistsOrInit(new File (this.dbFolder, FILE_NAME), deleteExistingDbs);

        LOG.info("Connecting to lmdb in folder {}...", this.dbFolder.getName());
        Env.Builder builder = Env.create()
                    .setMapSize(MAX_DB_SIZE)
                    .setMaxDbs(1);
        if (readOnly) {
            this.env = builder.open(this.dbFolder, EnvFlags.MDB_RDONLY_ENV);
        } else {
            this.env = builder.open(this.dbFolder);
        }
        List<String> databases = this.getDatabases();
        LOG.info("Found databases {}", databases);

        this.dbi = env.openDbi(dbName, DbiFlags.MDB_CREATE);
        LOG.info("Connected to database {}", dbName);
        return isConnected(dbName);
    }

    /**
     * Get a list of databases
     * @return list of databases in the lmdb file
     */
    public List<String> getDatabases() {
        List<byte[]> dbiNames = env.getDbiNames();
        LOG.info("Found {} databases", dbiNames.size());
        List<String> result = new ArrayList<>(dbiNames.size());
        for (byte[] dbiName : dbiNames) {
            result.add(new String(dbiName, CHARSET));
            if (result.size() > 100) {
                // to prevent memory issues while checking large files with lot of databases
                LOG.info("Listing only first 100 databases");
                break;
            }
        }
        return result;
    }

    /**
     * Check if we are connected to the provided database
     * @param dbName database name to check
     * @return true if connection is  made to the provided database, otherwise false
     */
    public boolean isConnected(String dbName) {
        if (dbName == null) {
            return dbi != null;
        }
        return dbi != null && new String(dbi.getName(), CHARSET).equals(dbName);
    }

    private void checkDbFileExistsOrInit(File dbFile, boolean deleteOldData) {
        LOG.info("Checking if lmdb file exists: {}...", dbFile.getAbsolutePath());
        boolean exists = dbFile.exists();
        if (exists) {
            LOG.info("Found lmdb file in folder {}", dbFile.getParentFile().getName());
            if (deleteOldData) {
                LOG.info("Deleting data file...");
                try {
                    Files.delete(Path.of(dbFile.getAbsolutePath()));
                } catch (IOException e)  {
                    throw new LmdbStateException("Cannot delete lmdb file", e);
                }
                exists = false;
            }
        }

        if (!exists) {
            LOG.info("File not found. Creating new lmdb folder {}...", dbFile.getParentFile());
            if (!dbFile.getParentFile().mkdirs() && dbFile.canWrite()) {
                throw new LmdbStateException("Cannot initialize lmdb file at " + dbFile.getAbsolutePath());
            }
        }
    }

    /**
     * Try to close an open database connection
     */
    public  void close() {
        LOG.info("Closing connection to local lmdb database...");
        if (dbi != null) {
            dbi.close();
            dbi = null;
        }
        if (env != null && !env.isClosed()) {
            env.close();
            env = null;
        }
    }

    /**
     * Return the number of items in the provided database
     * @return item count
     */
    public long getItemCount() {
        try (Txn<ByteBuffer> transaction = this.env.txnRead()) {
            Stat stat = this.dbi.stat(transaction);
            return stat.entries;
        }
    }

    /**
     * Retrieve a String value from the database
     * @param key key to read
     * @return read value as String
     */
    public String readString(Long key) {
        try (Txn<ByteBuffer> txn = this.env.txnRead()) {
            final ByteBuffer keyBuffer = longToByteBuffer(key);
            final ByteBuffer valueBuffer = this.dbi.get(txn, keyBuffer);
            return byteBufferToString(valueBuffer);
        }
    }

    /**
     * Retrieve a Long value from the database
     * @param key key to read
     * @return read value as Long
     */
    public Long readLong(String key) {
        try (Txn<ByteBuffer> txn = this.env.txnRead()) {
            final ByteBuffer keyBuffer = stringToByteBuffer(key);
            final ByteBuffer valueBuffer = this.dbi.get(txn, keyBuffer);
            return byteBufferToLong(valueBuffer);
        }
    }

    /**
     * Write a Long key and String value to the database. Strings longer than 511 bytes are truncated
     * The write is done as a single transaction.
     * @param key the key to write
     * @param value the value to write
     */
    public void write(Long key, String value) {
        this.dbi.put(Lmdb.longToByteBuffer(key), Lmdb.stringToByteBuffer(value));
    }

    /**
     * Write a String key and Long value to the database. Strings longer than 511 bytes are truncated
     * The write is done as a single transaction.
     * @param key the key to write
     * @param value the value to write
     */
    public void write(String key, Long value) {
        this.dbi.put(Lmdb.stringToByteBuffer(key), Lmdb.longToByteBuffer(value));
    }

    /**
     * Convert a String into a ByteBuffer. Strings longer than 511 bytes are truncated
     * @param str string to convert
     * @return generated ByteBuffer
     */
    public static ByteBuffer stringToByteBuffer(String str) {
        byte[] bytes = str.getBytes(CHARSET);
        if (bytes.length > MAX_KEY_SIZE) {
            LOG.warn("String {} will be truncated", str);
            bytes = Arrays.copyOf(bytes, MAX_KEY_SIZE);
        }
        final ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        buffer.put(bytes).flip();
        return buffer;
    }

    /**
     * Convert a ByteBuffer to a String
     * @param buffer the buffer to convert
     * @return String present in the buffer, null if buffer is null
     */
    public static String byteBufferToString(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        return CHARSET.decode(buffer).toString();
    }

    /**
     * Convert a Long into a ByteBuffer.
     * @param longValue long to convert
     * @return generated ByteBuffer
     */
    public static ByteBuffer longToByteBuffer(Long longValue) {
        return ByteBuffer.allocateDirect(Long.BYTES).putLong(longValue).flip();
    }

    /**
     * Convert a ByteBuffer to a Long value
     * @param buffer the buffer to convert
     * @return Long present in the buffer, null if buffer is null
     */
    public static Long byteBufferToLong(ByteBuffer buffer) {
        if (buffer == null || !buffer.hasRemaining()) {
            return null;
        }
        return buffer.getLong();
    }
}
