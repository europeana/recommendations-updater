package eu.europeana.api.recommend.updater.service.record;

import eu.europeana.api.recommend.updater.config.JobCmdLineStarter;
import eu.europeana.api.recommend.updater.config.JobData;
import eu.europeana.api.recommend.updater.exception.SolrException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Service that interacts with Solr engine to retrieve a list of available data sets
 */
@Service
public class SolrSetReader implements Tasklet, StepExecutionListener {

    private static final Logger LOG = LogManager.getLogger(SolrSetReader.class);

    private static final String DATASET_NAME = "edm_datasetName";
    private static final String TIMESTAMP_UPDATE = "timestamp_update";

    @Value("${zookeeper.url}")
    private String zookeeperURL;

    @Value("${solr.core}")
    private String solrCore;

    private CloudSolrClient client;
    private Date fromDate;
    private List<String> setsToDownload;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        // check job parameters
        fromDate = JobCmdLineStarter.getFromDate(stepExecution.getJobParameters());
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        connectToSolr();
        setsToDownload = getSets(fromDate);
        closeSolr();
        return RepeatStatus.FINISHED;
    }

    private void connectToSolr() throws SolrException {
        LOG.info("Connecting to Solr Zookeeper cluster: {}...", zookeeperURL);

        client = new CloudSolrClient.Builder(Arrays.asList(zookeeperURL.split(",")), Optional.empty())
                .build();
        client.setDefaultCollection(solrCore);
        client.connect();
        LOG.info("Connected to Solr {}", zookeeperURL);

        setsToDownload = getSets(fromDate);
        LOG.info("Found {} sets to download", setsToDownload.size());
    }

    private List<String> getSets(Date date) throws SolrException {
        SolrQuery query = new SolrQuery("*:*")
                .setRows(0)
                .setFields(DATASET_NAME)
                .setFacet(true)
                .setFacetLimit(5000) // set very high so we can get all sets (about 2200 atm) in 1 go
                .setFacetMinCount(1) // avoid listing empty or not-modified sets
                .addFacetField(DATASET_NAME);
        if (date != null) {
            String fromString = date.toInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            LOG.debug("Filtering sets updated after {}", fromString);
            query.addFilterQuery(TIMESTAMP_UPDATE + ":[" + fromString + "Z TO *]");
        }
        List<String> result = new ArrayList<>();
        try {
            QueryResponse response = client.query(query);
            FacetField setsFacet = response.getFacetField(DATASET_NAME);
            for (FacetField.Count facetField : setsFacet.getValues()) {
                result.add(facetField.getName());
            }
            return result;
        } catch (SolrServerException|IOException e) {
            throw new SolrException("Error retrieving list of sets from Solr", e);
        }
    }

    private void closeSolr() throws IOException {
        if (client != null) {
            LOG.info("Closing connection to Solr");
            client.close();
        }
    }

    /**
     * Save resuls to JobExecutionContext
     * @param stepExecution
     * @return step completed status
     */
    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        stepExecution.getJobExecution().getExecutionContext().put(JobData.SETS_KEY, setsToDownload);
        return ExitStatus.COMPLETED;
    }

}
