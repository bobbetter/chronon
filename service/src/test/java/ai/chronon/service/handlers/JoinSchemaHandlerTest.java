package ai.chronon.service.handlers;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaJoinSchemaResponse;
import ai.chronon.online.JoinCodec;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.RoutingContext;
import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class JoinSchemaHandlerTest {
    @Mock
    private JavaFetcher mockFetcher;

    @Mock
    private RoutingContext routingContext;

    @Mock
    private HttpServerResponse response;

    private JoinSchemaHandler handler;
    private Vertx vertx;

    @Before
    public void setUp(TestContext context) {
        MockitoAnnotations.openMocks(this);
        vertx = Vertx.vertx();

        handler = new JoinSchemaHandler(mockFetcher);

        // Set up common routing context behavior
        when(routingContext.response()).thenReturn(response);
        when(response.putHeader(anyString(), anyString())).thenReturn(response);
        when(response.setStatusCode(anyInt())).thenReturn(response);
        when(routingContext.pathParam("name")).thenReturn("test_join");
    }

    @Test
    public void testSuccessfulRequest(TestContext context) {
        Async async = context.async();

        String avroSchemaString = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
        String [] keys = {"user_id"};
        JavaJoinSchemaResponse.ValueInfo valueInfo = new JavaJoinSchemaResponse.ValueInfo("my_groupby_feature_1", "my_groupby", "", keys, "foo");
        JavaJoinSchemaResponse.ValueInfo[] valueInfos = {valueInfo};
        JavaJoinSchemaResponse joinSchemaResponse = new JavaJoinSchemaResponse("user_join", avroSchemaString, avroSchemaString, "fakeschemaHash", valueInfos);
        JTry<JavaJoinSchemaResponse> joinSchemaResponseTry = JTry.success(joinSchemaResponse);

        // Set up mocks
        when(mockFetcher.fetchJoinSchema(anyString())).thenReturn(joinSchemaResponseTry);

        // Capture the response that will be sent
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);

        // Trigger call
        handler.handle(routingContext);

        // Assert results
        vertx.setTimer(1000, id -> {
            verify(response).setStatusCode(200);
            verify(response).putHeader("content-type", "application/json");
            verify(response).end(responseCaptor.capture());

            // Compare response strings
            JsonObject actualResponse = new JsonObject(responseCaptor.getValue());

            String schemaHash = actualResponse.getString("schemaHash");
            context.assertEquals(schemaHash, "fakeschemaHash");

            String returnedJoinName = actualResponse.getString("joinName");
            context.assertEquals(returnedJoinName, "user_join");

            String keySchema = actualResponse.getString("keySchema");
            context.assertEquals(keySchema, avroSchemaString);

            String valueSchema = actualResponse.getString("valueSchema");
            context.assertEquals(valueSchema, avroSchemaString);

            // sanity check the value info payload
            JsonArray valueInfoArray = actualResponse.getJsonArray("valueInfos");
            context.assertEquals(valueInfoArray.size(), 1);
            JsonObject valueInfoJson = valueInfoArray.getJsonObject(0);
            context.assertEquals(valueInfoJson.getString("fullName"), "my_groupby_feature_1");
            JsonArray leftKeysArray = valueInfoJson.getJsonArray("leftKeys");
            context.assertEquals(leftKeysArray.size(), 1);
            context.assertEquals(leftKeysArray.getString(0), "user_id");

            // confirm we can parse the avro schema fine
            new Schema.Parser().parse(keySchema);
            new Schema.Parser().parse(valueSchema);
            async.complete();
        });
    }

    @Test
    public void testFailedRequest(TestContext context) {
        Async async = context.async();

        // Set up mocks
        JTry<JavaJoinSchemaResponse> joinSchemaResponseTry = JTry.failure(new Exception("some fake failure"));

        when(mockFetcher.fetchJoinSchema(anyString())).thenReturn(joinSchemaResponseTry);

        // Capture the response that will be sent
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);

        // Trigger call
        handler.handle(routingContext);

        // Assert results
        vertx.setTimer(1000, id -> {
            verify(response).setStatusCode(500);
            verify(response).putHeader("content-type", "application/json");
            verify(response).end(responseCaptor.capture());

            // Verify response format
            validateFailureResponse(responseCaptor.getValue(), context);
            async.complete();
        });
    }

    private void validateFailureResponse(String jsonResponse, TestContext context) {
        JsonObject actualResponse = new JsonObject(jsonResponse);
        context.assertTrue(actualResponse.containsKey("errors"));

        String failureString = actualResponse.getJsonArray("errors").getString(0);
        context.assertNotNull(failureString);
    }
}
