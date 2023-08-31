package eu.europeana.api.recommend.updater.service.record;

import eu.europeana.api.recommend.common.RecordId;
import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import eu.europeana.api.recommend.common.model.EmbeddingRecord;
import eu.europeana.api.recommend.updater.model.record.Entity;
import eu.europeana.api.recommend.updater.model.record.Proxy;
import eu.europeana.api.recommend.updater.model.record.Record;
import eu.europeana.api.recommend.updater.util.AverageTime;
import eu.europeana.api.recommend.updater.util.StringLimitUtils;
import eu.europeana.api.recommend.updater.util.UriUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Pulls the relevant data out of a record and puts it in a new EmbeddingRecord object (for sending to the
 * Embeddings API)
 *
 * @author Patrick Ehlert
 */
@Component
public class RecordToEmbedRecordProcessor implements ItemProcessor<List<Record>, List<EmbeddingRecord>> {

    private static final Logger LOG = LogManager.getLogger(RecordToEmbedRecordProcessor.class);

    private static final int MAX_VALUES = 100; // only take the first 100 values to prevent issues (some records have a lot of data)
    private static final int MAX_VALUE_LENGTH = 3000; // maximum number of characters for a value

    private static final String ENGLISH = "en";
    private static final String DEF = "def";
    private AverageTime averageTime;

    public RecordToEmbedRecordProcessor(UpdaterSettings settings) {
        if (settings != null) {
            this.averageTime = new AverageTime(settings.getLogTimingInterval(), "creating EmbedRecords"); // for debugging purposes
        }
    }

    @Override
    public List<EmbeddingRecord> process(final List<Record> records) {
        long start = System.currentTimeMillis();
        List<EmbeddingRecord> result = new ArrayList<>(records.size());
        for (Record rec : records) {
            LOG.trace("Processing record {}, lastModified = {}", rec.getAbout(), rec.getTimestampUpdated());

            // gather all entities for easy reference
            List<Entity> entities = new ArrayList<>();
            entities.addAll(createEntityList(rec.getAgents()));
            entities.addAll(createEntityList(rec.getConcepts()));
            entities.addAll(createEntityList(rec.getPlaces()));
            entities.addAll(createEntityList(rec.getTimespans()));

            String milvusRecId = new RecordId(rec.getAbout()).getMilvusId();
            Collection<String> title = new ArrayList<>();
            Collection<String> description = new ArrayList<>();
            Collection<String> creator = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            Collection<String> tags = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            Collection<String> places = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            Collection<String> times = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            for (Proxy p : rec.getProxies()) {
                addAllValues(title, p.getDcTitle(), null, true, "dcTitle", milvusRecId);
                addAllValues(title, p.getDctermsAlternative(), null, true, "dcTermsAlternative", milvusRecId);

                addAllValues(description, p.getDcDescription(), null, true, "dcDescription", milvusRecId);

                addAllValues(creator, p.getDcCreator(), entities, true, "dcCreator", milvusRecId);
                addAllValues(creator, p.getDcContributor(), entities, true, "dcContributor", milvusRecId);
                addAllValues(creator, p.getDcSubject(), createEntityList(rec.getAgents()), false, "dcSubject (agents)", milvusRecId);
                addAllValues(creator, p.getEdmHasMet(), createEntityList(rec.getAgents()), false, "dcEdmHasMet (agents)", milvusRecId);

                addAllValues(tags, p.getDcType(), entities, true, "dcType", milvusRecId);
                addAllValues(tags, p.getDctermsMedium(), entities, true, "dctermsMedium", milvusRecId);
                addAllValues(tags, p.getDcFormat(), entities, true, "dcFormat", milvusRecId);
                addAllValues(tags, p.getDcSubject(), createEntityList(rec.getConcepts()), true, "dcSubject", milvusRecId);

                addAllValues(places, p.getDctermsSpatial(), entities, true, "dcTermsSpatial", milvusRecId);
                addAllValues(places, p.getEdmCurrentLocation(), entities, true, "edmCurrentLocation", milvusRecId);
                addAllValues(places, p.getDcSubject(), createEntityList(rec.getPlaces()), false, "dcSubject (places)", milvusRecId);

                addAllValues(times, p.getDctermsCreated(), entities, true, "dctermsCreated", milvusRecId);
                addAllValues(times, p.getDctermsIssued(), entities, true, "dctermsIssued", milvusRecId);
                addAllValues(times, p.getDctermsTemporal(), entities, true, "dctermsTemporal", milvusRecId);
                addAllValues(times, p.getEdmHasMet(), createEntityList(rec.getTimespans()), false, "edmHasMet (timespans)", milvusRecId);
            }

            EmbeddingRecord embedRecord = new EmbeddingRecord(
                    milvusRecId, // required field
                    title.toArray(new String[0]), // required field
                    description.toArray(new String[0]),
                    creator.toArray(new String[0]),
                    tags.toArray(new String[0]),
                    places.toArray(new String[0]),
                    times.toArray(new String[0]));
            LOG.trace("{}", embedRecord);
            result.add(embedRecord);
        }
        if (LOG.isDebugEnabled()) {
            long duration = System.currentTimeMillis() - start;
            averageTime.addTiming(duration);
            LOG.trace("2. Generated {} EmbeddingRecords in {} ms", result.size(), duration);
        }
        return result;
    }

    private List<Entity> createEntityList(List<? extends Entity> list) {
        if (list == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(list);
    }

    /**
     * Adds all values from the provided source map to the provided target list. Note that we only pick values from 1
     * language; English or else def, or else the first language we find
     * If the list of entities is not null then we also check for uris. For value that is an uri and that refers to an
     * existing entity we add the entities prefLabels and altLabels instead.
     */
    private void addAllValues(Collection<String> target, Map<String, List<String>> source, List<Entity> entities,
                              boolean addLiterals, String fieldName, String recordId) {
        if (source == null || source.keySet().isEmpty()) {
            return;
        }
        if (target.size() > MAX_VALUES) {
            LOG.warn("Maximum number of values {} reached reading field {} of record {}", MAX_VALUES, fieldName, recordId);
            return;
        }

        // pick values from 1 language (including all uri's found in def field)
        String fieldNameRecordId = fieldName + " of record " + recordId;
        if (source.containsKey(ENGLISH)) {
            addValuesAndResolveUris(target, source.get(ENGLISH), entities, addLiterals, fieldNameRecordId);
            // always include uri's from def field (always exclude non-uri def field values)
            if (entities != null && source.containsKey(DEF)) {
                addValuesAndResolveUris(target, source.get(DEF), entities, false, fieldNameRecordId);
            }
        } else if (source.containsKey(DEF)) {
            addValuesAndResolveUris(target, source.get(DEF), entities, addLiterals, fieldNameRecordId);
        } else {
            List<String> values = source.values().iterator().next(); // pick any language
            addValuesAndResolveUris(target, values, entities, addLiterals, fieldNameRecordId);
        }
    }

    private void addValuesAndResolveUris(Collection<String> target, List<String> values, List<Entity> entities, boolean addLiterals,
                                         String description) {
        for (String value : values) {
            if (target.size() > MAX_VALUES) {
                break;
            }
            if (UriUtils.isUri(value)) {
                resolveEntityUri(target, value, entities);
            } else if (addLiterals) {
                String toAdd = value;
                if (value.length() > MAX_VALUE_LENGTH) {
                    toAdd = StringLimitUtils.limit(value, MAX_VALUE_LENGTH);
                    LOG.warn("Reducing number of characters of {} from {} to {}", description, value.length(), toAdd.length());
                }
                target.add(toAdd);
            }
        }
    }

    private void resolveEntityUri(Collection<String> target, String value, List<Entity> entities) {
        if (entities == null) {
            return;
        }
        for (Entity entity : entities) {
            if (target.size() > MAX_VALUES) {
                break;
            }
            if (value.equalsIgnoreCase(entity.getAbout())) {
                String prefLabel = getFirstPrefLabel(entity);
                LOG.trace("  Uri {} refers to entity {} with preflabel {}", value, entity.getAbout(), prefLabel);
                if (StringUtils.isNotBlank(prefLabel)) {
                    target.add(prefLabel);
                }
            }
        }
    }
    
    /**
     * Return the first found preflabel of an entity. If there are English prefLabels, we pick the first from that.
     * Otherwise we pick one in the first language we find.
     */
    private String getFirstPrefLabel(Entity entity) {
        List<String> labels = getLabel(entity.getPrefLabel(), entity.getAbout());
        if (labels.isEmpty()) {
            return null;
        }
        return labels.get(0);
    }

    private List<String> getLabel(Map<String, List<String>> languageMap, String entityId) {
        if (languageMap != null && !languageMap.isEmpty()) {
            if (languageMap.containsKey(ENGLISH)) {
                return languageMap.get(ENGLISH);
            } else {
                LOG.trace("No English preflabel for entity {}", entityId);
                return languageMap.values().iterator().next();
            }
        } else {
            LOG.trace("No preflabels for entity {}",  entityId);
        }
        return Collections.emptyList();
    }

}
