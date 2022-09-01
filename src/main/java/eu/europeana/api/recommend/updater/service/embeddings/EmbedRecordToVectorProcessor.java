package eu.europeana.api.recommend.updater.service.embeddings;

import eu.europeana.api.recommend.updater.config.BuildInfo;
import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import eu.europeana.api.recommend.updater.exception.EmbeddingsException;
import eu.europeana.api.recommend.updater.model.embeddings.EmbeddingRecord;
import eu.europeana.api.recommend.updater.model.embeddings.EmbeddingRequestData;
import eu.europeana.api.recommend.updater.model.embeddings.EmbeddingResponse;
import eu.europeana.api.recommend.updater.model.embeddings.RecordVectors;
import eu.europeana.api.recommend.updater.util.AverageTime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Send EmbedRecord objects to the Embeddings API. The Embeddings API returns vectors that can be saved in Milvus
 *
 * @author Patrick Ehlert
 */
@Component
public class EmbedRecordToVectorProcessor implements ItemProcessor<List<EmbeddingRecord>, List<RecordVectors>> {

    private static final Logger LOG = LogManager.getLogger(EmbedRecordToVectorProcessor.class);

    // on each retry it will add extra wait time, so with 5 retries with wait time 2 sec then the application
    // will fail after 2 + 4 + 6 + 8 + 10 = 30 seconds
    private static final int RETRY_GET_CLIENT = 5;
    private static final int RETRY_GET_CLIENT_WAIT_TIME = 2; // in seconds
    // 7 retries, wait time 3 sec -> 3 + 6 + 9 + 12 + 15 + 18 + 21 = 84 seconds
    private static final int RETRY_GET_VECTOR = 7;
    private static final int RETRY_GET_VECTOR_WAIT_TIME = 3; // in seconds

    private static final long MS_PER_SEC = 1000;

    private static final int TIMEOUT = 40; // in seconds

    private static final int MAX_RESPONSE_SIZE_MB = 10;
    private static final int BYTES_PER_MB = 1024 * 1024;

    private final UpdaterSettings settings;
    private final BuildInfo buildInfo;

    private final Queue<WebClient> webClients = new ConcurrentLinkedQueue<>();

    private boolean shuttingDown = false;
    private AverageTime averageTime; // for debugging purposes

    public EmbedRecordToVectorProcessor(UpdaterSettings settings, BuildInfo buildInfo) {
        this.settings = settings;
        this.buildInfo = buildInfo;
        if (LOG.isDebugEnabled()) {
            this.averageTime = new AverageTime(settings.getLogTimingInterval(), "sending/receiving from Embeddings API");
        }
    }

    @PostConstruct
    private void initWebClient() {
        // Check whether to use 1 Embeddings API address or multiple
        // If multiple, we'll keep track of which one was used last so load balancing is improved
        String[] embeddingsApis = settings.getEmbeddingsApiUrl().split(",");
        if (embeddingsApis.length == 1) {
            webClients.add(createWebClient(settings.getEmbeddingsApiUrl()));
            LOG.info("Using 1 Embeddings API address at {}", webClients.peek());
        } else {
            LOG.info("Multiple Embeddings API addresses found");
            for (String embeddingApi : embeddingsApis) {
                String url = embeddingApi.trim();
                LOG.info("  {}", url);
                webClients.add(createWebClient(url));
            }
            // Also check if number of threads in config match the number of addresses
            if (settings.getThreads() != embeddingsApis.length) {
                LOG.warn("Found {} Embeddings API urls, but application is configured to use {} threads", embeddingsApis.length, settings.getBatchSize());
            }
        }
    }

    private WebClient createWebClient(String url) {
        WebClient.Builder wcBuilder = WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(HttpClient.create()
                        .compress(true)
                        .responseTimeout(Duration.ofSeconds(TIMEOUT))))
                .exchangeStrategies(ExchangeStrategies.builder()
                        .codecs(configurer -> configurer
                                .defaultCodecs()
                                .maxInMemorySize(MAX_RESPONSE_SIZE_MB * BYTES_PER_MB))
                        .build());

        return wcBuilder
                .baseUrl(url)
                .defaultHeader(HttpHeaders.USER_AGENT, buildInfo.getAppName() + " v" + buildInfo.getAppVersion())
                .filter(logRequest())
                .filter(logResponse())
                .build();
    }


    private ExchangeFilterFunction logRequest() {
        return (request, next) -> {
            LOG.trace("Request: {} {}", request.method(), request.url());
            return next.exchange(request);
        };
    }

    private ExchangeFilterFunction logResponse() {
        return ExchangeFilterFunction.ofResponseProcessor(response -> {
            LOG.trace("Response: {} {}", response.statusCode().value(), response.statusCode().getReasonPhrase());
            return Mono.just(response);

        });
    }

    @Override
    public List<RecordVectors> process(List<EmbeddingRecord> embeddingRecords) throws InterruptedException, EmbeddingsException {
        LOG.trace("Sending {} records to Embedding API...", embeddingRecords.size());
        return retrySend(embeddingRecords, RETRY_GET_VECTOR);
    }

    private List<RecordVectors> retrySend(List<EmbeddingRecord> embeddingRecords, int maxTries) throws InterruptedException, EmbeddingsException {
        int nrTries = 1;
        EmbeddingResponse response = null;
        List<RecordVectors> result = null;

        // Extra check because Embeddings API will fail if we sent 0 items to it.
        if (embeddingRecords == null || embeddingRecords.isEmpty()) {
            LOG.warn("No items to sent to Embeddings API");
            return result;
        }

        while (nrTries <= maxTries && response == null) {
            Long start = System.currentTimeMillis();
            try {
                response = getVectors(embeddingRecords.toArray(new EmbeddingRecord[0])).block();
                long duration = System.currentTimeMillis() - start;
                if (LOG.isDebugEnabled()) {
                    averageTime.addTiming(duration);
                }

                result = (response == null ? null : Arrays.asList(response.getData()));
                LOG.trace("  Response = {}...", result);
                if (result == null) {
                    LOG.warn("No response from Embeddings API after {} ms!", duration);
                } else {
                    LOG.trace("3. Generated {} vectors in {} ms", result.size(), duration);
                }
            } catch (RuntimeException e) {
                String setName;
                if (embeddingRecords.isEmpty()) {
                    setName = "unknown - empty list of embeddings records!";
                } else {
                    setName = getSetName(embeddingRecords.get(0));
                }
                LOG.warn("Request to Embeddings API for set {} failed after {} ms with error {} and cause {}. Attempt {}, retrying....",
                        setName, System.currentTimeMillis() - start, e.getMessage(), e.getCause(), nrTries);
                if (shuttingDown || nrTries == maxTries) {
                    throw e; // rethrow so error is propagated
                } else {
                    int sleepTime = RETRY_GET_VECTOR_WAIT_TIME * nrTries;
                    LOG.warn("Holding off thread for set {} for {} seconds", setName, sleepTime);
                    Thread.sleep(sleepTime * MS_PER_SEC); // wait some extra time before we try again
                }
            }
            nrTries++;
        }
        return result;
    }

    private String getSetName(EmbeddingRecord embeddingRecord) {
        String result;
        if (embeddingRecord == null) {
            result = "unknown - provided record is null";
        } else {
            String[] parts = embeddingRecord.getId().split("/");
            if (parts.length > 0) {
                result = parts[0];
            } else {
                result = "unknown - id doesn't contain expected / character";
            }
        }
        return result;
    }

    /**
     * Check if there is a webclient that is not in use. If so we return that, else we wait until one is
     * available
     * @return
     */
    private synchronized WebClient getWebClientFromQueue(int maxTries) throws InterruptedException, EmbeddingsException {
        int nrTries = 1;
        WebClient result = null;

        while (result == null && nrTries < maxTries){
            result = webClients.poll();
            if (result == null) {
                int sleepTime = RETRY_GET_CLIENT_WAIT_TIME * nrTries;
                LOG.warn("All Embeddings API instances are in use. Waiting {} sec ...", sleepTime);
                Thread.sleep(sleepTime * MS_PER_SEC); // wait some extra time before we try again
                nrTries++;
            }
        }

        if (result == null) {
            throw new EmbeddingsException("No Embeddings API address available. Giving up");
        }
        return result;
    }

    private Mono<EmbeddingResponse> getVectors(EmbeddingRecord[] embeddingRecords) throws InterruptedException, EmbeddingsException {
        WebClient webClient = null;
        try {
            webClient = getWebClientFromQueue(RETRY_GET_CLIENT);
            return webClient.post()
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                    .bodyValue(new EmbeddingRequestData(embeddingRecords))
                    .retrieve()
                    .bodyToMono(EmbeddingResponse.class);
        } finally {
            if (webClient != null) {
                this.webClients.add(webClient); // put back in queue
            }
        }
    }

}
