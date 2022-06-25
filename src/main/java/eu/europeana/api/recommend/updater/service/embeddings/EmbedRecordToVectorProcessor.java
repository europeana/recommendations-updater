package eu.europeana.api.recommend.updater.service.embeddings;

import eu.europeana.api.recommend.updater.config.BuildInfo;
import eu.europeana.api.recommend.updater.config.UpdaterSettings;
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
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * Send EmbedRecord objects to the Embeddings API. The Embeddings API returns vectors that can be saved in Milvus
 *
 * @author Patrick Ehlert
 */
@Component
public class EmbedRecordToVectorProcessor implements ItemProcessor<List<EmbeddingRecord>, List<RecordVectors>> {

    private static final Logger LOG = LogManager.getLogger(EmbedRecordToVectorProcessor.class);

    private static final int RETRIES = 5;
    private static final int RETRY_WAIT_TIME = 45; // in seconds
    private static final int TIMEOUT = 70; // in seconds
    // note that the current implementation of Embeddings API may terminate connections after 50 seconds

    private static final int MAX_RESPONSE_SIZE_MB = 10;
    private static final int BYTES_PER_MB = 1024 * 1024;

    private final UpdaterSettings settings;
    private final BuildInfo buildInfo;
    private WebClient webClient;
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
        WebClient.Builder wcBuilder = WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(HttpClient.create()
                        .compress(true)
                        .responseTimeout(Duration.ofSeconds(TIMEOUT))))
                .exchangeStrategies(ExchangeStrategies.builder()
                .codecs(configurer -> configurer
                        .defaultCodecs()
                        .maxInMemorySize(MAX_RESPONSE_SIZE_MB * BYTES_PER_MB))
                .build());

        this.webClient = wcBuilder
                .baseUrl(settings.getEmbeddingsApiUrl())
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
    public List<RecordVectors> process(List<EmbeddingRecord> embeddingRecords) throws InterruptedException {
        LOG.trace("Sending {} records to Embedding API...", embeddingRecords.size());
        return retrySend(embeddingRecords, RETRIES);
    }

    private List<RecordVectors> retrySend(List<EmbeddingRecord> embeddingRecords, int maxTries) throws InterruptedException {
        int nrTries = 1;
        EmbeddingResponse response = null;
        List<RecordVectors> result = null;

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
                    int sleepTime = RETRY_WAIT_TIME * 1000 * nrTries;
                    LOG.warn("Holding off thread for set {} for {} seconds", setName, sleepTime/1000);
                    Thread.sleep(sleepTime); // wait some extra time before we try again
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
            if (parts.length > 1) {
                result = parts[1];
            } else {
                result = "unknown - id doesn't contain expected / character";
            }
        }
        return result;
    }

    private Mono<EmbeddingResponse> getVectors(EmbeddingRecord[] embeddingRecords) {
        return webClient.post()
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .bodyValue(new EmbeddingRequestData(embeddingRecords))
                .retrieve()
                .bodyToMono(EmbeddingResponse.class);
    }

    @PreDestroy
    @SuppressWarnings({"fb-contrib:USFW_UNSYNCHRONIZED_SINGLETON_FIELD_WRITES"})
    public void close() throws InterruptedException {
        shuttingDown = true;
        LOG.info("Waiting 20 seconds to shut down webclient...");
        Thread.sleep(20_000); // allow connections to finish to prevent issues with Embeddings API failing
        LOG.info("Webclient closed.");
    }
}
