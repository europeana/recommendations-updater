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
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;

@Component
public class EmbedRecordToVectorProcessor implements ItemProcessor<List<EmbeddingRecord>, List<RecordVectors>> {

    private static final Logger LOG = LogManager.getLogger(EmbedRecordToVectorProcessor.class);

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
            LOG.debug("Request: {} {}", request.method(), request.url());
            return next.exchange(request);
        };
    }

    private ExchangeFilterFunction logResponse() {
        return ExchangeFilterFunction.ofResponseProcessor(response -> {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Response: {} {}", response.statusCode().value(), response.statusCode().getReasonPhrase());
            }
            return Mono.just(response);

        });
    }

    private Mono<EmbeddingResponse> getVectors(EmbeddingRecord[] embeddingRecords) {
        return webClient.post()
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .bodyValue(new EmbeddingRequestData(embeddingRecords))
                .retrieve()
                .bodyToMono(EmbeddingResponse.class);
    }

    @Override
    public List<RecordVectors> process(List<EmbeddingRecord> embeddingRecords) {
        LOG.debug("Sending {} records to Embedding API...", embeddingRecords.size());
        EmbeddingResponse response = getVectors(embeddingRecords.toArray(new EmbeddingRecord[0])).block();
        List<RecordVectors> result = (response == null ? null : Arrays.asList(response.getData()));
        LOG.trace("  Response = {}...", result);
        return result;
    }
}
