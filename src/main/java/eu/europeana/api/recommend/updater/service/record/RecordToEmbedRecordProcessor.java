package eu.europeana.api.recommend.updater.service.record;

import eu.europeana.api.recommend.updater.model.embeddings.EmbeddingRecord;
import eu.europeana.api.recommend.updater.model.record.Entity;
import eu.europeana.api.recommend.updater.model.record.Proxy;
import eu.europeana.api.recommend.updater.model.record.Record;
import eu.europeana.api.recommend.updater.util.UriUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Pulls the relevant data out of a record and puts it in a new EmbeddingRecord object (for sending to the
 * Embeddings API)
 *
 * @author Patrick Ehlert
 */
@Component
public class RecordToEmbedRecordProcessor implements ItemProcessor<List<Record>, List<EmbeddingRecord>> {

    private static final Logger LOG = LogManager.getLogger(RecordToEmbedRecordProcessor.class);

    private static final String ENGLISH = "en";
    private static final String DEF = "def";

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

            String id = rec.getAbout();
            List<String> title = new ArrayList<>();
            List<String> description = new ArrayList<>();
            List<String> creator = new ArrayList<>();
            List<String> tags = new ArrayList<>();
            List<String> places = new ArrayList<>();
            List<String> times = new ArrayList<>();
            for (Proxy p : rec.getProxies()) {
                addAllValues(title, p.getDcTitle(), null, true);
                addAllValues(title, p.getDctermsAlternative(), null, true);

                addAllValues(description, p.getDcDescription(), null, true);

                addAllValues(creator, p.getDcCreator(), entities, true);
                addAllValues(creator, p.getDcContributor(), entities, true);
                addAllValues(creator, p.getDcSubject(), createEntityList(rec.getAgents()), false);
                addAllValues(creator, p.getEdmHasMet(), createEntityList(rec.getAgents()), false);

                addAllValues(tags, p.getDcType(), entities, true);
                addAllValues(tags, p.getDctermsMedium(), entities, true);
                addAllValues(tags, p.getDcFormat(), entities, true);
                addAllValues(tags, p.getDcSubject(), createEntityList(rec.getConcepts()), true);

                addAllValues(places, p.getDctermsSpatial(), entities, true);
                addAllValues(places, p.getEdmCurrentLocation(), entities, true);
                addAllValues(places, p.getDcSubject(), createEntityList(rec.getPlaces()), false);

                addAllValues(times, p.getDctermsCreated(), entities, true);
                addAllValues(times, p.getDctermsIssued(), entities, true);
                addAllValues(times, p.getDctermsTemporal(), entities, true);
                addAllValues(times, p.getEdmHasMet(), createEntityList(rec.getTimespans()), false);
            }

            EmbeddingRecord embedRecord = new EmbeddingRecord(
                    id, // required field
                    title.toArray(new String[0]), // required field
                    description.toArray(new String[0]),
                    creator.toArray(new String[0]),
                    tags.toArray(new String[0]),
                    places.toArray(new String[0]),
                    times.toArray(new String[0]));
            LOG.trace("{}", embedRecord);
            result.add(embedRecord);
        }
        LOG.debug("2. Generated {} EmbeddingRecords in {} ms", result.size(), System.currentTimeMillis() - start);
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
    private void addAllValues(List<String> target, Map<String, List<String>> source, List<Entity> entities, boolean addLiterals) {
        if (source == null || source.keySet().isEmpty()) {
            return;
        }
        // pick values from 1 language (including all uri's found in def field)
        if (source.containsKey(ENGLISH)) {
            addValuesAndResolveUris(target, source.get(ENGLISH), entities, addLiterals);
            // always include uri's from def field (always exclude non-uri def field values)
            if (entities != null && source.containsKey(DEF)) {
                addValuesAndResolveUris(target, source.get(DEF), entities, false);
            }
        } else if (source.containsKey(DEF)) {
            addValuesAndResolveUris(target, source.get(DEF), entities, addLiterals);
        } else {
            List<String> values = source.values().iterator().next(); // pick any language
            addValuesAndResolveUris(target, values, entities, addLiterals);
        }
    }

    private void addValuesAndResolveUris(List<String> target, List<String> values, List<Entity> entities, boolean addLiterals) {
        for (String value : values) {
            addValueAndResolveUri(target, value, entities, addLiterals);
        }
    }

    private void addValueAndResolveUri(List<String> target, String value, List<Entity> entities, boolean addLiterals) {
        if (UriUtils.isUri(value)) {
            if (entities == null) {
                return;
            }
            for (Entity entity : entities) {
                if (value.equalsIgnoreCase(entity.getAbout())) {
                    LOG.trace("  Uri {} refers to entity {} -> adding entity label...", value, entity.getAbout());
                    String prefLabel = getFirstPrefLabel(entity);
                    if (StringUtils.isNotBlank(prefLabel)) {
                        target.add(prefLabel);
                    }
                }
            }
        } else if (addLiterals) {
            target.add(value);
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
