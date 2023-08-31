package eu.europeana.api.recommend.updater.service.record;

import eu.europeana.api.recommend.updater.model.record.Record;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;

/**
 * Service to read data from Mongo. The quickest way to retrieve records per set is to use a 'starts-with' regex on the
 * 'about' field.
 *
 * We use an approach were we use the mongoId of the last retrieved record to get the next batch of records for a
 * particular set. This means that we are limited to 1 thread per set. To guarantee a consistent order we order by
 * MongoId field.
 */
@Service
public class MongoService {

    private static final String FIELD_ID = "_id";
    private static final String FIELD_ABOUT = "about";
    private static final String FIELD_TIMESTAMP_UPDATED = "timestampUpdated";
    private static final String FIELD_QUALITY_ANNOTATIONS = "qualityAnnotations.body";
    private static final String VALUE_REGEX_CONTENT_TIER0 = ".*contentTier0$";

    private MongoTemplate mongoTemplate;

    @Autowired
    public MongoService(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    /**
     *
     * @param aboutRegex regex filtering per set, should be in the form of "^/<setId>/"
     * @param lastRetrieved id of the last retrieve mongo object, null to start with a set
     * @param pageSize number of items to retrieve
     * @return list of retrieved records from Mongo
     */
    public List<Record> getAllRecordsPaged(String aboutRegex, String lastRetrieved, long pageSize) {
        // we order by mongo _id field, so we can easily keep track of the last retrieved object and continue
        // with the next objects in the next chunk without performance loss
        Criteria criteria = Criteria.where(FIELD_ABOUT).regex(aboutRegex)
                .and(FIELD_QUALITY_ANNOTATIONS).not().regex(VALUE_REGEX_CONTENT_TIER0);
        return getRecords(lastRetrieved, (int) pageSize, criteria);
    }

    /**
     *
     * @param aboutRegex regex filtering per set, should be in the form of "^/<setId>/"
     * @param updatedAfter filters records on lastModified date
     * @param lastRetrieved id of the last retrieve mongo object, null to start with a set
     * @param pageSize number of items to retrieve
     * @return  list of retrieved records from Mongo
     */
    public List<Record> getAllRecordsPagedUpdatedAfter(String aboutRegex, Date updatedAfter, String lastRetrieved, long pageSize) {
        Criteria criteria = Criteria.where(FIELD_ABOUT).regex(aboutRegex)
                .and(FIELD_QUALITY_ANNOTATIONS).not().regex(VALUE_REGEX_CONTENT_TIER0)
                .and(FIELD_TIMESTAMP_UPDATED).gt(updatedAfter);
        return getRecords(lastRetrieved, (int) pageSize, criteria);
    }

    private List<Record> getRecords(String lastRetrieved, int pageSize, Criteria criteria) {
        if (lastRetrieved != null) {
            criteria = criteria.and(FIELD_ID).gt(new ObjectId(lastRetrieved));
        }

        Query query = new Query(criteria);
        query.with(Sort.by(Sort.Direction.ASC, FIELD_ID)).limit(pageSize);
        return mongoTemplate.find(query, Record.class);
    }

    /**
     * @return total number of records not ContentTier 0 in mongo
     */
    public long countAll() {
        Criteria criteria = Criteria.where(FIELD_QUALITY_ANNOTATIONS).not().regex(VALUE_REGEX_CONTENT_TIER0);
        return mongoTemplate.count(new Query(criteria), Record.class);
    }

    /**
     *
     * @param updatedAfter
     * @return total number of records not ContentTier 0 updated after the provided date
     */
    public long countAllUpdatedAfter(Date updatedAfter) {
        Criteria criteria = Criteria.where(FIELD_TIMESTAMP_UPDATED).gt(updatedAfter)
                .and(FIELD_QUALITY_ANNOTATIONS).not().regex(VALUE_REGEX_CONTENT_TIER0);
        return mongoTemplate.count(new Query(criteria), Record.class);
    }

    /**
     *
     * @param aboutRegex
     * @return total number of records not ContentTier 0 in the list of provided sets
     */
    public long countAllAboutRegex(String aboutRegex) {
        Criteria criteria = Criteria.where(FIELD_ABOUT).regex(aboutRegex)
                .and(FIELD_QUALITY_ANNOTATIONS).not().regex(VALUE_REGEX_CONTENT_TIER0);
        return mongoTemplate.count(new Query(criteria), Record.class);
    }

}
