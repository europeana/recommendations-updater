package eu.europeana.api.recommend.updater;

import eu.europeana.api.recommend.updater.model.embeddings.EmbeddingRecord;
import eu.europeana.api.recommend.updater.model.record.*;
import eu.europeana.api.recommend.updater.service.record.RecordToEmbedRecordProcessor;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RecordToEmbedRecordProcessorTest {

    private static final String ABOUT = "/test/1";
    private static final String DC_TITLE1 = "This is a title";
    private static final String DC_TITLE2 = "Second title";
    private static final String DC_TERMS_ALTERNATIVE = "Alternative";
    private static final String DC_DESCRIPTION = "This is a description";
    private static final String DC_CREATOR = "A. Rtist";
    private static final String DC_CONTRIBUTOR =" C. Ontributor";
    private static final String DC_SUBJECT = "test";
    private static final String DC_TERMS_MEDIUM = "medium";
    private static final String DC_TERMS_SPATIAL = "right here, right now";
    private static final String EDM_HAS_MET = "someone";

    private static final String AGENT_ABOUT = "http://data.europeana.eu/agent/base/007";
    private static final String AGENT_PREFLABEL_EN = "James Bond";
    private static final String AGENT_PREFLABEL_CN = "占士邦";
    private static final String AGENT_ALTLABEL_EN = "Agent 007";
    private static final String AGENT_ALTLABEL_CN = "特工007";

    private static final String CONCEPT_ABOUT = "http://data.europeana.eu/concept/base/1";
    private static final String CONCEPT_ALTLABEL_NL = "Film";

    private static final String TIMESPAN1_ABOUT = "http://semium.org/time/1977";
    private static final String TIMESPAN1_ALTLABEL_EN = "Somewhere back in the 70s";
    private static final String TIMESPAN2_ABOUT = "http://semium.org/time/AD2xxx";
    private static final String TIMESPAN2_PREFLABEL_EN = "Second millenium AD";

    /**
     * Test to see if there are nullpointers when certain information is not available
     * Assumption is that records have an id and at least 1 proxy
     */
    @Test
    public void testMinimal() {
        Record record = new Record();
        record.setAbout(ABOUT);
        record.setProxies(Collections.singletonList(new Proxy()));
        EmbeddingRecord result = new RecordToEmbedRecordProcessor().process(Collections.singletonList(record)).get(0);

        assertEquals(ABOUT, result.getId());
    }

    @Test
    public void testTitleSingle(){
        // test record by default has dcTitle and dcTermsAlternative, so we strip out one
        Record record = createTestRecord();
        record.getProxies().get(0).setDcTitle(null);
        EmbeddingRecord result = new RecordToEmbedRecordProcessor().process(Collections.singletonList(record)).get(0);

        assertEquals(1, result.getTitle().length);
        assertEquals(DC_TERMS_ALTERNATIVE, result.getTitle()[0]);

        record = createTestRecord();
        record.getProxies().get(0).setDctermsAlternative(null);
        result = new RecordToEmbedRecordProcessor().process(Collections.singletonList(record)).get(0);

        assertEquals(1, result.getTitle().length);
        assertEquals(DC_TITLE1, result.getTitle()[0]);
    }

    @Test
    public void testTitleDouble(){
        Record record = createTestRecord();
        record.getProxies().get(0).setDcTitle(new HashMap<>() {{
            put("en", Collections.singletonList(DC_TITLE1));
            put("fr", Collections.singletonList(DC_TITLE2)); // if we have English then French is ignored
            put("def", Collections.singletonList(AGENT_ABOUT)); // uris should be ignored
        }});
        EmbeddingRecord result = new RecordToEmbedRecordProcessor().process(Collections.singletonList(record)).get(0);

        assertEquals(2, result.getTitle().length);
        assertEquals(DC_TITLE1, result.getTitle()[0]);
        assertEquals(DC_TERMS_ALTERNATIVE, result.getTitle()[1]);

        // Now we should have 3 titles
        record.getProxies().get(0).setDcTitle(new HashMap<>() {{
            put("en", Arrays.asList(DC_TITLE1, DC_TITLE2));
        }});
        result = new RecordToEmbedRecordProcessor().process(Collections.singletonList(record)).get(0);
        assertEquals(3, result.getTitle().length);
        assertEquals(DC_TITLE1, result.getTitle()[0]);
        assertEquals(DC_TITLE2, result.getTitle()[1]);
        assertEquals(DC_TERMS_ALTERNATIVE, result.getTitle()[2]);
    }

    @Test
    public void testDescription(){
        Record record = createTestRecord();
        EmbeddingRecord result = new RecordToEmbedRecordProcessor().process(Collections.singletonList(record)).get(0);

        // should not matter what language it is in, as long as there is a description
        record.getProxies().get(0).setDcDescription(new HashMap<>() {{
            put("nl", Collections.singletonList(DC_DESCRIPTION));
        }});

        assertEquals(1, result.getDescription().length);
        assertEquals(DC_DESCRIPTION, result.getDescription()[0]);
    }

    @Test
    public void testCreatorSingle(){
        // test record by default has dcCreator and dcContributor, so we strip out one
        Record record = createTestRecord();
        record.getProxies().get(0).setDcContributor(null);
        EmbeddingRecord result = new RecordToEmbedRecordProcessor().process(Collections.singletonList(record)).get(0);

        assertEquals(1, result.getCreator().length);
        assertEquals(DC_CREATOR, result.getCreator()[0]);

        record = createTestRecord();
        record.getProxies().get(0).setDcCreator(null);
        result = new RecordToEmbedRecordProcessor().process(Collections.singletonList(record)).get(0);

        assertEquals(1, result.getCreator().length);
        assertEquals(DC_CONTRIBUTOR, result.getCreator()[0]);
    }

    @Test
    public void testCreatorLinkedAgent(){
        Record record = createTestRecord();
        // For dcSubject we also include referred agents, but not other entities
        record.getProxies().get(0).setDcSubject(new HashMap<>() {{
            put("def", Arrays.asList(AGENT_ABOUT, CONCEPT_ABOUT));
        }});
        EmbeddingRecord result = new RecordToEmbedRecordProcessor().process(Collections.singletonList(record)).get(0);

        assertEquals(4, result.getCreator().length);
        assertEquals(DC_CREATOR, result.getCreator()[0]);
        assertEquals(DC_CONTRIBUTOR, result.getCreator()[1]);
        assertEquals(AGENT_PREFLABEL_EN, result.getCreator()[2]);
        assertEquals(AGENT_ALTLABEL_EN, result.getCreator()[3]);
    }

    @Test
    public void testCreatorLinkedEntity(){
        Record record = createTestRecord();
        // for dcCreator (and dcContributor) all referred entities need to be included
        record.getProxies().get(0).setDcCreator(new HashMap<>() {{
            put("en", Collections.singletonList(DC_CREATOR));
            put("def", Arrays.asList(AGENT_ABOUT, CONCEPT_ABOUT));
        }});
        EmbeddingRecord result = new RecordToEmbedRecordProcessor().process(Collections.singletonList(record)).get(0);

        assertEquals(5, result.getCreator().length);
        assertEquals(DC_CREATOR, result.getCreator()[0]);
        assertEquals(AGENT_PREFLABEL_EN, result.getCreator()[1]);
        assertEquals(AGENT_ALTLABEL_EN, result.getCreator()[2]);
        assertEquals(CONCEPT_ALTLABEL_NL, result.getCreator()[3]);
        assertEquals(DC_CONTRIBUTOR, result.getCreator()[4]);
    }


    @Test
    public void testTags() {
        Record record = createTestRecord();
        EmbeddingRecord result = new RecordToEmbedRecordProcessor().process(Collections.singletonList(record)).get(0);

        assertEquals(2, result.getTags().length);
        assertEquals(DC_TERMS_MEDIUM, result.getTags()[0]);
        assertEquals(DC_SUBJECT, result.getTags()[1]);
    }

    @Test
    public void testTagsLinkedConcept() {
        Record record = createTestRecord();
        record.getProxies().get(0).setDcSubject(new HashMap<>() {{
            put("def", Arrays.asList(AGENT_ABOUT, CONCEPT_ABOUT)); // referred concepts should be included, other entities not
            put("en", Collections.singletonList(DC_SUBJECT));
        }});
        EmbeddingRecord result = new RecordToEmbedRecordProcessor().process(Collections.singletonList(record)).get(0);

        assertEquals(3, result.getTags().length);
        assertEquals(DC_TERMS_MEDIUM, result.getTags()[0]);
        assertEquals(DC_SUBJECT, result.getTags()[1]);
        assertEquals(CONCEPT_ALTLABEL_NL, result.getTags()[2]);
    }

    @Test
    public void testPlaces() {
        Record record = createTestRecord();
        EmbeddingRecord result = new RecordToEmbedRecordProcessor().process(Collections.singletonList(record)).get(0);

        assertEquals(1, result.getPlaces().length);
        assertEquals(DC_TERMS_SPATIAL, result.getPlaces()[0]);
    }

    @Test
    public void testTimesEmpty() {
        Record record = createTestRecord();
        EmbeddingRecord result = new RecordToEmbedRecordProcessor().process(Collections.singletonList(record)).get(0);

        // edmHasMet does not refer to any timespan
        assertEquals(0, result.getTimes().length);
    }

    @Test
    public void testTimesLinkedTimespans() {
        Record record = createTestRecord();
        record.getProxies().get(0).getEdmHasMet().put("def", Arrays.asList(TIMESPAN1_ABOUT, TIMESPAN2_ABOUT, "http://notfound.com"));
        EmbeddingRecord result = new RecordToEmbedRecordProcessor().process(Collections.singletonList(record)).get(0);

        assertEquals(2, result.getTimes().length);
        assertEquals(TIMESPAN1_ALTLABEL_EN, result.getTimes()[0]);
        assertEquals(TIMESPAN2_PREFLABEL_EN, result.getTimes()[1]);
    }

    @Test
    public void testMultipleProxy() {
        Record record = createTestRecord();
        record.getProxies().get(0).setDctermsCreated(new HashMap<>() {{
            put("def", Arrays.asList(TIMESPAN1_ABOUT, TIMESPAN2_ABOUT, "http://notfound.com"));
        }});
        Proxy proxy2 = new Proxy(); // add empty proxy
        proxy2.setAbout("/proxy/test/2");
        Proxy proxy3 = new Proxy(); // add proxy with additional timespan info
        proxy3.setAbout("/proxy/test/3");
        proxy3.setDctermsCreated(new HashMap<>() {{
            put("def", Collections.singletonList("1891"));
        }});
        record.setProxies(Arrays.asList(record.getProxies().get(0), proxy2, proxy3));

        EmbeddingRecord result = new RecordToEmbedRecordProcessor().process(Collections.singletonList(record)).get(0);
        assertEquals(3, result.getTimes().length);
        assertEquals(TIMESPAN1_ALTLABEL_EN, result.getTimes()[0]);
        assertEquals(TIMESPAN2_PREFLABEL_EN, result.getTimes()[1]);
        assertEquals("1891", result.getTimes()[2]);
    }

   private Record createTestRecord() {
        Proxy proxy = new Proxy();
        proxy.setAbout("/proxy/test/1");
        proxy.setDcTitle(new HashMap<>() {{
            put("en", Collections.singletonList(DC_TITLE1));
        }});
        proxy.setDctermsAlternative(new HashMap<>() {{
            put("en", Collections.singletonList(DC_TERMS_ALTERNATIVE));
        }});
        proxy.setDcDescription(new HashMap<>() {{
            put("en", Collections.singletonList(DC_DESCRIPTION));
        }});
        proxy.setDcCreator(new HashMap<>() {{
            put("en", Collections.singletonList(DC_CREATOR));
        }});
        proxy.setDcCreator(new HashMap<>() {{
            put("en", Collections.singletonList(DC_CREATOR));
        }});
        proxy.setDcContributor(new HashMap<>() {{
            put("en", Collections.singletonList(DC_CONTRIBUTOR));
        }});
        proxy.setDcSubject(new HashMap<>() {{
            put("en", Collections.singletonList(DC_SUBJECT));
        }});
        proxy.setDctermsMedium(new HashMap<>() {{
            put("en", Collections.singletonList(DC_TERMS_MEDIUM));
        }});
        proxy.setDctermsSpatial(new HashMap<>() {{
            put("en", Collections.singletonList(DC_TERMS_SPATIAL));
        }});
        proxy.setEdmHasMet(new HashMap<>() {{
            put("en", Collections.singletonList(EDM_HAS_MET));
        }});

        Agent agent = new Agent();
        agent.setAbout(AGENT_ABOUT);
        agent.setPrefLabel(new HashMap<>() {{
            put("cn", Collections.singletonList(AGENT_PREFLABEL_CN));
            put("en", Collections.singletonList(AGENT_PREFLABEL_EN));
        }});
        agent.setAltLabel(new HashMap<>() {{
            put("cn", Collections.singletonList(AGENT_ALTLABEL_CN));
            put("en", Collections.singletonList(AGENT_ALTLABEL_EN));
        }});

        Concept concept = new Concept();
        concept.setAbout(CONCEPT_ABOUT);
        concept.setAltLabel(new HashMap<>() {{
            put("nl", Collections.singletonList(CONCEPT_ALTLABEL_NL));
        }});

        Timespan timespan1 = new Timespan();
        timespan1.setAbout(TIMESPAN1_ABOUT);
        timespan1.setAltLabel(new HashMap<>() {{
                put("en", Collections.singletonList(TIMESPAN1_ALTLABEL_EN));
        }});
        Timespan timespan2 = new Timespan();
        timespan2.setAbout(TIMESPAN2_ABOUT);
        timespan2.setPrefLabel(new HashMap<>() {{
            put("en", Collections.singletonList(TIMESPAN2_PREFLABEL_EN));
        }});

        Record record = new Record();
        record.setAbout(ABOUT);
        record.setProxies(Collections.singletonList(proxy));
        record.setAgents(Collections.singletonList(agent));
        record.setConcepts(Collections.singletonList(concept));
        record.setTimespans(Arrays.asList(timespan1, timespan2));

        return record;
    }

}
