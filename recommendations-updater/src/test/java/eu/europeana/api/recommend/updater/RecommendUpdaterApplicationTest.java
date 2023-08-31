package eu.europeana.api.recommend.updater;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.BootstrapWith;

/**
 * Basic test for loading context
 */
@ActiveProfiles("test") // to avoid initialising Mongo (see also application-test.yml)
@SpringBootTest
@BootstrapWith(NoJobStart.class)
class RecommendUpdaterApplicationTest {

    @MockBean
    private MongoTemplate mongoTemplate;

    @SuppressWarnings("squid:S2699") // we are aware that this test doesn't have any assertion
    @Test
    void contextLoads() {
    }


}
