package eu.europeana.api.recommend.updater.service.embeddings;

import eu.europeana.api.recommend.updater.config.BuildInfo;
import eu.europeana.api.recommend.updater.config.UpdaterSettings;
import eu.europeana.api.recommend.updater.model.embeddings.EmbeddingRecord;
import eu.europeana.api.recommend.updater.model.embeddings.EmbeddingRequestData;
import eu.europeana.api.recommend.updater.model.embeddings.EmbeddingResponse;
import eu.europeana.api.recommend.updater.model.embeddings.RecordVectors;
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

@Component
public class EmbedRecordToVectorProcessor implements ItemProcessor<List<EmbeddingRecord>, List<RecordVectors>> {

    private static final Logger LOG = LogManager.getLogger(EmbedRecordToVectorProcessor.class);

    private static final int RETRIES = 3;
    private static final int TIMEOUT = 90; // seconds

    private static final int MAX_RESPONSE_SIZE_MB = 10;
    private static final int BYTES_PER_MB = 1024 * 1024;

    private final UpdaterSettings settings;
    private final BuildInfo buildInfo;
    private WebClient webClient;

    public EmbedRecordToVectorProcessor(UpdaterSettings settings, BuildInfo buildInfo) {
        this.settings = settings;
        this.buildInfo = buildInfo;
    }

    @PostConstruct
    private void initWebClient() {
        WebClient.Builder wcBuilder = WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(HttpClient.create()
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
            if (LOG.isDebugEnabled()) {
                LOG.trace("Response: {} {}", response.statusCode().value(), response.statusCode().getReasonPhrase());
            }
            return Mono.just(response);

        });
    }

    @Override
    public List<RecordVectors> process(List<EmbeddingRecord> embeddingRecords) {
        LOG.trace("Sending {} records to Embedding API...", embeddingRecords.size());
        return retrySend(embeddingRecords, RETRIES);
    }

    private List<RecordVectors> retrySend(List<EmbeddingRecord> embeddingRecords, int maxTries) {
        int nrTries = 1;
        EmbeddingResponse response = null;
        List<RecordVectors> result = null;

        while (nrTries <= maxTries && response == null) {
            Long start = System.currentTimeMillis();
            try {
                response = getVectors(embeddingRecords.toArray(new EmbeddingRecord[0])).block();
                result = (response == null ? null : Arrays.asList(response.getData()));
                LOG.trace("  Response = {}...", result);
                if (result == null) {
                    LOG.warn("No response from Embeddings API after {} ms!", System.currentTimeMillis() - start);
                } else {
                    LOG.debug("3. Generated {} vectors in {} ms", result.size(), System.currentTimeMillis() - start);
                }
            } catch (RuntimeException e) {
                LOG.warn("Request to Embeddings API failed after {} ms! Retrying....",System.currentTimeMillis() - start, e);
                if (nrTries == maxTries) {
                    throw e; // rethrow so error is propagated
                }
            }
            nrTries++;
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
}
