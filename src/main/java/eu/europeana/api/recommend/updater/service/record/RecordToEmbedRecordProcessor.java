package eu.europeana.api.recommend.updater.service.record;

import eu.europeana.api.recommend.updater.model.embeddings.EmbeddingRecord;
import eu.europeana.api.recommend.updater.model.record.Entity;
import eu.europeana.api.recommend.updater.model.record.Proxy;
import eu.europeana.api.recommend.updater.model.record.Record;
import eu.europeana.api.recommend.updater.util.UriUtils;
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
public class RecordToEmbedRecordProcessor implements ItemProcessor<Record, EmbeddingRecord> {

    private static final Logger LOG = LogManager.getLogger(RecordToEmbedRecordProcessor.class);

    private static final String ENGLISH = "en";
    private static final String DEF = "def";

    @Override
    public EmbeddingRecord process(final Record record) {
        LOG.debug("Processing record {}, created = {}, lastModified = {}", record.getAbout(),
                record.getTimestampCreated(), record.getTimestampUpdated());

        // gather all entities for easy reference
        List<Entity> entities = new ArrayList<>();
        entities.addAll(record.getAgents());
        entities.addAll(record.getConcepts());
        entities.addAll(record.getPlaces());
        entities.addAll(record.getTimespans());

        String id = record.getAbout();
        List<String> title = new ArrayList<>();
        List<String> description = new ArrayList<>();
        List<String> creator = new ArrayList<>();
        List<String> tags = new ArrayList<>();
        List<String> places = new ArrayList<>();
        List<String> times = new ArrayList<>();
        for (Proxy p : record.getProxies()) {
            addAllValues(title, p.getDcTitle(), null, true);
            addAllValues(title, p.getDctermsAlternative(), null, true);

            addAllValues(description, p.getDcDescription(), null, true);

            addAllValues(creator, p.getDcCreator(), entities, true);
            addAllValues(creator, p.getDcContributor(), entities, true);
            addAllValues(creator, p.getDcSubject(), new ArrayList<>(record.getAgents()), false);
            addAllValues(creator, p.getEdmHasMet(), new ArrayList<>(record.getAgents()), false);

            addAllValues(tags, p.getDcType(), entities, true);
            addAllValues(tags, p.getDctermsMedium(), entities, true);
            addAllValues(tags, p.getDcFormat(), entities, true);
            addAllValues(tags, p.getDcSubject(), new ArrayList<>(record.getConcepts()), true);

            addAllValues(places, p.getDctermsSpatial(), entities, true);
            addAllValues(places, p.getEdmCurrentLocation(), entities, true);
            addAllValues(places, p.getDcSubject(), new ArrayList<>(record.getPlaces()), false);

            addAllValues(times, p.getDctermsCreated(), entities, true);
            addAllValues(times, p.getDctermsIssued(), entities, true);
            addAllValues(times, p.getDctermsTemporal(), entities, true);
            addAllValues(times, p.getEdmHasMet(), new ArrayList<>(record.getTimespans()), false);
        }

        EmbeddingRecord result = new EmbeddingRecord(id, // required field
                title.toArray(new String[0]),
                description.toArray(new String[0]),
                creator.toArray(new String[0]),
                tags.toArray(new String[0]),
                places.toArray(new String[0]),
                times.toArray(new String[0]));
        LOG.trace("{}", result);
        return result;
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
                    LOG.trace("  Uri {} refers to entity {} -> adding entity labels", value, entity.getAbout());
                    target.addAll(getEnglishPrefAndAltLabel(entity));
                }
            }
        } else if (addLiterals) {
            target.add(value);
        }
    }

    /**
     * Return the English prefLabel and altLabel of an entity (or the labels in any other language when there is no
     * English label).
     */
    private List<String> getEnglishPrefAndAltLabel(Entity entity) {
        List<String> result = new ArrayList<>();
        result.addAll(getLabel(entity.getPrefLabel(), "prefLabels", entity.getAbout()));
        result.addAll(getLabel(entity.getAltLabel(), "altLabels", entity.getAbout()));
        return result;
    }

    private List<String> getLabel(Map<String, List<String>> languageMap, String labelName, String entityId) {
        if (languageMap != null && !languageMap.isEmpty()) {
            if (languageMap.containsKey(ENGLISH)) {
                return languageMap.get(ENGLISH);
            } else {
                LOG.warn("No English {} for entity {}", labelName, entityId);
                return languageMap.values().iterator().next();
            }
        } else {
            LOG.warn("No {} for entity {}", labelName, entityId);
        }
        return Collections.emptyList();
    }

}
