package eu.europeana.api.recommend.updater.service.milvus;

import org.apache.logging.log4j.LogManager;
import org.springframework.batch.core.*;
import org.springframework.stereotype.Service;

@Service
public class MilvusService implements StepExecutionListener {

    @Override
    public void beforeStep(StepExecution stepExecution) {
        LogManager.getLogger(MilvusService.class).info("Before step");
        JobParameters jobParameters = stepExecution.getJobParameters();

        // TODO for full update check if configured collection is empty
        // TODO for partial update check if configured collection exists and has contents
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        LogManager.getLogger(MilvusService.class).info("After step");
        return null;
    }
}
