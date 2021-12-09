package eu.europeana.api.recommend.updater;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.BootstrapWith;

/**
 * Basic test for loading context
 */
@SpringBootTest
@BootstrapWith(NoJobStart.class)
class RecommendUpdaterApplicationTest {

    @SuppressWarnings("squid:S2699") // we are aware that this test doesn't have any assertion
    @Test
    void contextLoads() {
    }


}
