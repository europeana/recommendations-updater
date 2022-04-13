package eu.europeana.api.recommend.updater.service.milvus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class LmdbTest {

    private static final Logger LOG = LogManager.getLogger(LmdbTest.class);

    private static final String DB_NAME = null; // we use unnamed databases (null) for most or our tests

    private static File tempDb;
    private static Lmdb lmdb;

    @BeforeEach
    public void setupDb(@TempDir Path tempDir) {
        tempDb = tempDir.toFile();
        lmdb = new Lmdb(tempDb, false);
        assertTrue(lmdb.connect(DB_NAME));
    }

    @AfterEach
    public void closeDb() {
        lmdb.close();
        tempDb.delete();
    }

    @Test
    public void testWriteAndReadShortString() {
        Long key = 1_000_000L;
        String value = "a value";
        LOG.info("Saving string with length {} bytes", value.getBytes(StandardCharsets.UTF_8).length);
        this.lmdb.write(key, value);
        assertEquals(value, this.lmdb.readString(key));

        String value2 = "changed value"; // replace value
        this.lmdb.write(key, value2);
        assertEquals(value2, this.lmdb.readString(key));
    }

    /**
     * We store at most 510 bytes of a string in LMDB, so test that we can read back at least first part of a long string
     */
    @Test
    public void testWriteAndReadTruncatedString() {
        Long key = 20L;
        StringBuilder s = new StringBuilder("//datasetId/1_")
                .append("2".repeat(585)) // 585 just to make a nice 600 bytes string
                .append("3");
        String value = s.toString();
        String expected = s.substring(0, Lmdb.MAX_KEY_SIZE);
        LOG.info("Saving string with length {} bytes", value.getBytes(StandardCharsets.UTF_8).length);

        this.lmdb.write(key, value);
        String readValue = this.lmdb.readString(key);
        assertEquals(Lmdb.MAX_KEY_SIZE, readValue.length());
        assertEquals(expected, readValue);
    }

    @Test
    public void testWriteKeyAndReadLong() {
        String key = "x";
        Long value = 2L;
        this.lmdb.write(key, value);
        assertEquals(value, this.lmdb.readLong(key));

        Long value2 = 100L; // replace value
        this.lmdb.write(key, value2);
        assertEquals(value2, this.lmdb.readLong(key));
    }

    @Test
    public void testWriteTruncatedKeyAndReadLong() {
        StringBuilder s = new StringBuilder("a")
                .append("b".repeat(598))
                .append("c");
        String key = s.toString();
        LOG.info("Saving string with length {} bytes", key.getBytes(StandardCharsets.UTF_8).length);
        Long value = 50_000_000L;

        this.lmdb.write(key, value);
        assertEquals(value, this.lmdb.readLong(key));
    }

    @Test
    public void testReadWriteStrings() {
        String key = "9999";
        String value = "//datasetId/recordId";

        this.lmdb.write(key, value);
        String readValue = this.lmdb.readString(key);
        assertEquals(value, readValue);
    }

    @Test
    public void testReadWriteMultipleStrings() {
        List<String> keys = List.of("a", "b", "c", "d");
        List<String> ids  = List.of("1", "2", "3", "4");
        this.lmdb.write(keys, ids);
        assertEquals("1", this.lmdb.readString("a"));
        assertEquals("2", this.lmdb.readString("b"));
        assertEquals("3", this.lmdb.readString("c"));
        assertEquals("4", this.lmdb.readString("d"));
    }

    @Test
    public void testReadWriteMultipleStringsFail() {
        List<String> keys = List.of("a", "b", "c", "d");
        List<String> ids  = List.of("1", "2", "3", "4", "5");
        assertThrows(IllegalArgumentException.class, () -> this.lmdb.write(keys, ids));
    }

    @Test
    public void testItemCount() {
        assertEquals(0, this.lmdb.getItemCount());
        this.lmdb.write("test", 1L);
        assertEquals(1, this.lmdb.getItemCount());
        this.lmdb.write("test2", 2L);
        assertEquals(2, this.lmdb.getItemCount());

        this.lmdb.write(10L, "test 10");
        assertEquals(3, this.lmdb.getItemCount());
        this.lmdb.write(11L, "test 11");
        assertEquals(4, this.lmdb.getItemCount());
    }

    @Test
    public void testReconnectAndDeleteContents() {
        this.lmdb.write(1L, "test");
        this.lmdb.write("test2", 2L);
        assertEquals(2, this.lmdb.getItemCount());
        this.lmdb.close();

        this.lmdb.connect(DB_NAME, false);
        assertEquals(2, this.lmdb.getItemCount());
        this.lmdb.close();

        this.lmdb.connect(DB_NAME, true);
        assertEquals(0, this.lmdb.getItemCount());
    }

    @Test
    public void testNotFoundString() {
        assertNull(this.lmdb.readLong("/datasetId/recordId"));
    }

    @Test
    public void testNotFoundLong() {
        assertNull(this.lmdb.readString(1_000_000L));
    }

    @Test
    public void testNamedDatabase() {
        Lmdb lmdb = new Lmdb(tempDb, false);
        lmdb.connect("database1");
        assertTrue(lmdb.isConnected("database1"));
        assertFalse(lmdb.isConnected(null));
        lmdb.close();

        lmdb.connect("database2");
        assertEquals(2, lmdb.getDatabases().size());
        assertFalse(lmdb.isConnected("database1"));
        lmdb.close();
    }
}
