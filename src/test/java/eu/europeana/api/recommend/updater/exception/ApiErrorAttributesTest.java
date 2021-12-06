package eu.europeana.api.recommend.updater.exception;

import io.micrometer.core.instrument.util.StringUtils;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

/**
 * Test error responses
 * MockMvc does not support testing error response contents, that's why we use RestAssured here instead
 * Note that these tests depend on the error settings defined in application.yml
 */
@ActiveProfiles("test") // to load application-test.yml
@SpringBootTest(webEnvironment = RANDOM_PORT)
public class ApiErrorAttributesTest {

    @LocalServerPort
    private int port;

    @BeforeEach
    public void setUp() {
        RestAssured.port = port;
    }

    @Test
    public void testErrorFieldNoStacktraceNoMessage() {
        String path = "/myApi/error?param1=value1";
        JsonPath response = given().
                header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE).
                get(path).
        then().
                contentType(ContentType.JSON).extract().response().jsonPath();

        assertFalse(response.getBoolean("success"));
        // Some version return timestamp as an integer instead of ISO date
        assertTrue(response.getString("timestamp").contains("T"));
        assertEquals(HttpStatus.I_AM_A_TEAPOT.value(), response.getInt("status"));
        assertEquals("I'm a teapot", response.getString("error"));
        assertTrue(response.getString("message").isEmpty());
        assertEquals(path, response.getString("path"));
        assertNull(response.getString("trace"));
    }

    @Test
    public void testWithStacktrace() {
        String path = "/myApi/error?param1=value1&debug";
        JsonPath response = given().
                header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE).
                get(path).
        then().
                contentType(ContentType.JSON).extract().response().jsonPath();

        assertTrue(StringUtils.isNotBlank(response.getString("trace")));
    }

    @Test
    public void testWithMessage() {
        String path = "/myApi/error?param1=value1&debug&message";
        JsonPath response = given().
                header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE).
                get(path).
                then().
                contentType(ContentType.JSON).extract().response().jsonPath();

        assertFalse(response.getString("message").isEmpty());
    }
}
