package eu.europeana.api.recommend.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RecordIdTest {

    @Test
    public void recordIdEuropeanaTest() {
        String datasetId = "0123x";
        String localId = "test_2";
        RecordId rId = new RecordId(datasetId, localId);
        assertEquals("/0123x/test_2", rId.getEuropeanaId());
    }

    @Test
    public void recordIdEuropeana2Test() {
        String europeanaId = "/0123x/test_2";
        RecordId rId = new RecordId(europeanaId);
        assertEquals(europeanaId, rId.getEuropeanaId());
    }

    @Test
    public void recordIdSetTest() {
        String recordIdFromSet = "http://data.europeana.eu/item/11648/_Botany_L_1444675";
        assertEquals("/11648/_Botany_L_1444675", new RecordId(recordIdFromSet).getEuropeanaId());
    }

    @Test
    public void recordIdMilvusTest() {
        String datasetId = "0123x";
        String localId = "test_2";
        RecordId rId = new RecordId(datasetId, localId);
        assertEquals("0123x/test_2", rId.getMilvusId());
    }

    @Test
    public void recordIdMilvusTestTooLong() {
        String datasetId = "0";
        String localId = "012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
        RecordId rId = new RecordId(datasetId, localId);
        String expected = datasetId + "/" + localId.substring(0, 255-1-datasetId.length());
        assertEquals(expected, rId.getMilvusId());
    }

    @Test
    public void recordIdMilvusQuotesTest() {
        String datasetId = "0123x";
        String localId = "test_2";
        RecordId rId = new RecordId(datasetId, localId);
        assertEquals("'0123x/test_2'", rId.getMilvusIdQuotes());
    }

    @Test
    public void recordIdFromMilvusTest() {
        RecordId rId = new RecordId("0123x/test_2");
        assertEquals("/0123x/test_2", rId.getEuropeanaId());
    }

    @Test
    public void recordIdEquals() {
        RecordId rId1 = new RecordId("1", "2");
        RecordId rId2 = new RecordId("1", "2");
        assertTrue(rId1.equals(rId2));
    }

}
