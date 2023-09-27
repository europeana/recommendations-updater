package eu.europeana.api.recommend.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * In Milvus recordIds are stored a bit differently. To save space we don't store the leading slash and we only use
 * the first 100 characters of a record (these should uniquely identify a record).
 * This class saves the original Europeana record Id and can output the various parts and ways required.
 */
public class RecordId implements Serializable {

    public static final String SET_RECORD_ID_PREFIX = "http://data.europeana.eu/item/";
    public static final int MAX_SIZE = 100;

    @Serial
    private static final long serialVersionUID = 3143509414537273784L;
    private static final Logger LOG = LogManager.getLogger(RecordId.class);

    private final String dataSetId;
    private final String localId;

    /**
     * Create a new RecordId object
     * @param datasetId string describing the dataset name (first part of EuropeanaId)
     * @param localId string describing the local id (second part of EuropeanaId)
     */
    public RecordId(String datasetId, String localId) {
        this.dataSetId = datasetId;
        this.localId = localId;
    }

    /**
     * Give an EuropeanaId in some for, create a RecordId object. At the moment this supports 2 format:
     * <ol>
     *     <li>Id from set API response, e.g. http://data.europeana.eu/item/1234/5678</li>
     *     <li>EuropeanaId, e.g. /1234/5678</li>
     *     <li>Id as stored in Milvus, e.g. 1234/5678. Note that these ids could be incomplete, since Milvus only stores
     *     the first 100 characters of an id</li>
     * </ol></li>
     *
     * @param recordId so formatted as <datasetId>/<localId>
     */
    public RecordId(String recordId) {
        if (recordId.startsWith(SET_RECORD_ID_PREFIX)) {
            // process id from Set API
            recordId = recordId.substring(SET_RECORD_ID_PREFIX.length());
        } else if (recordId.startsWith("/")) {
            // process Europeana id
            recordId = recordId.substring(1);
        } else if (recordId.length() == MAX_SIZE) {
            // MilvusId if it's exactly 100 characters, most likely it was truncated
            LOG.warn("RecordId retrieved from Milvus may be incomplete: {}", recordId);
        }

        String[] split = recordId.split("/");
        this.dataSetId = split[0];
        if (split.length == 1) {
            LOG.warn("RecordId retrieved from Milvus does not contain slash: {}", recordId);
            this.localId = null;
        } else {
            this.localId = split[1];
        }
    }

    public String getDataSetId() {
        return dataSetId;
    }

    public String getEuropeanaId() {
        return '/' + dataSetId + '/' + localId;
    }

    /**
     * @return recordId milvus-style, so without the leading slash and maximum 100 characters
     */
    public String getMilvusId() {
        String result = dataSetId + "/" + localId;
        if (result.length() > MAX_SIZE) {
            LOG.warn("RecordId length longer than {} characters: {}", MAX_SIZE, result);
            return result.substring(0, MAX_SIZE);
        }
        return result;
    }

    /**
     * @return recordId milvus-style, embedded by single quotes
     */
    public String getMilvusIdQuotes() {
        return '\'' + getMilvusId() + '\'';
    }

    public String toString() {
        return getMilvusId();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordId recordId = (RecordId) o;
        return dataSetId.equals(recordId.dataSetId) && localId.equals(recordId.localId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSetId, localId);
    }
}
