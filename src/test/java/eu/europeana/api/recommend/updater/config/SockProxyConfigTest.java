package eu.europeana.api.recommend.updater.config;

import eu.europeana.api.recommend.updater.util.SocksProxyActivator;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test if loading sock proxy configuration works fine
 */
public class SockProxyConfigTest {

    @Test
    public void testPropertiesNotPresent() {
        SocksProxyConfig config = new SocksProxyConfig("notpresent.properties");
        assertFalse(config.isSocksEnabled());
    }

    /**
     * Test if loading multiple property files (first one where socks is enabled and then one where it is disabled) works
     */
    @Test
    public void testPropertiesDisabled() {
        SocksProxyConfig config = new SocksProxyConfig("socks_config_enabled.properties",
                "socks_config_disabled.properties");
        assertFalse(config.isSocksEnabled());
    }

    /**
     * Test if loading multiple property files (first one where socks is disabled and then one where it is enabled) works
     */
    @Test
    public void testPropertiesEnabled() {
        SocksProxyConfig config = new SocksProxyConfig("socks_config_disabled.properties",
                "socks_config_enabled.properties");
        assertTrue(config.isSocksEnabled());
        assertEquals("test.com", config.getHost());
        assertEquals("12345", config.getPort());
        assertEquals("user", config.getUser());
        assertEquals("secret", config.getPassword());
    }

    /**
     * Tests if a connection to google.com is blocked (because the proxy config is pointing to an non-existing server)
     */
    @Test
    public void testConnectionBlockedByProxy() throws IOException {
        SocksProxyConfig config = new SocksProxyConfig("socks_config_enabled.properties");
        SocksProxyActivator.activate(config);

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(2000)
                .setConnectTimeout(2000)
                .setSocketTimeout(2000)
                .build();
        int maxRetries = 1;
        try (CloseableHttpClient httpClient = HttpClients.custom()
                .setRetryHandler((exception, executionCount, context) -> executionCount <= maxRetries)
                .setDefaultRequestConfig(requestConfig)
                .build()) {
            HttpHead request = new HttpHead("http://www.google.com");

            try (CloseableHttpResponse response = httpClient.execute(request)) {
                // code should not reach this point
                assert false;
            } catch (SocketException e) {
                // we expect a SocketException when the connection attempt fails
                LogManager.getLogger(SockProxyConfigTest.class).info("Exception thrown as expected: {}", e.getMessage());
            }
        }
    }

    /**
     * Tests if a connection to google.com is allowed, because we configured it to bypass the non-existing proxy for
     * www.google.com using our socks.nonProxyHosts configuration option
     */
    @Test
    public void testConnectionNotViaProxy() throws IOException {
        SocksProxyConfig config = new SocksProxyConfig("socks_config_enabled_not_google.properties");
        SocksProxyActivator.activate(config);

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpHead request = new HttpHead("http://www.google.com");
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                assertEquals(200, response.getStatusLine().getStatusCode());
            }
        }
    }

}
