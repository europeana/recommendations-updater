package eu.europeana.api.recommend.updater.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SetUtilsTest {

    @Test
    public void testSetName() {
        assertEquals("2021503", SetUtils.datasetNameToId("2021503_Ag_BG_Ontotext_hdl_pub_624"));
        assertEquals("2022702", SetUtils.datasetNameToId("2022702b_Ag_ES_Hispana_esebvph1"));
    }

    @Test void testSetId() {
        assertEquals("2021503", SetUtils.datasetNameToId("2021503"));
        assertEquals("00781", SetUtils.datasetNameToId("00781"));
    }
}
