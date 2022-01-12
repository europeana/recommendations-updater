package eu.europeana.api.recommend.updater.service;

import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.stereotype.Service;

/**
 * Regulates the number of threads used to run the batch job
 */
@Service
public class TaskExecutor extends SimpleAsyncTaskExecutor {

    public TaskExecutor(UpdaterSettings settings) {
        this.setConcurrencyLimit(settings.getThreads());
        this.setThreadNamePrefix("Update");
    }

}
