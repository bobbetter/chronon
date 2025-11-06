package ai.chronon.service.handlers;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaRequest;
import ai.chronon.online.JavaResponse;
import ai.chronon.service.model.GetFeaturesResponse;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.RoutingContext;
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
public class JoinListHandlerTest {
    @Mock
    private JavaFetcher mockFetcher;

    @Mock
    private RoutingContext routingContext;

    @Mock
    private HttpServerResponse response;

    private JoinListHandler handler;
    private Vertx vertx;

    @Before
    public void setUp(TestContext context) {
        MockitoAnnotations.openMocks(this);
        vertx = Vertx.vertx();

        handler = new JoinListHandler(mockFetcher);

        // Set up common routing context behavior
        when(routingContext.response()).thenReturn(response);
        when(response.putHeader(anyString(), anyString())).thenReturn(response);
        when(response.setStatusCode(anyInt())).thenReturn(response);
    }

    @Test
    public void testSuccessfulRequest(TestContext context) {
        Async async = context.async();

        List<String> joins = List.of("my_joins.join_a.v1", "my_joins.join_a.v2", "my_joins.join_b.v1");
        // Set up mocks
        CompletableFuture<List<String>> futureListResponse =
                CompletableFuture.completedFuture(joins);

        when(mockFetcher.listJoins(anyBoolean())).thenReturn(futureListResponse);

        // Capture the response that will be sent
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);

        // Trigger call
        handler.handle(routingContext);

        // Assert results
        vertx.setTimer(1000, id -> {
            verify(response).setStatusCode(200);
            verify(response).putHeader("content-type", "application/json");
            verify(response).end(responseCaptor.capture());

            // Verify response format
            JsonObject actualResponse = new JsonObject(responseCaptor.getValue());
            JsonArray joinNames = actualResponse.getJsonArray("joinNames");
            context.assertEquals(joinNames.size(), joins.size());
            for (int i = 0; i < joinNames.size(); i++) {
                context.assertEquals(joins.get(i), joinNames.getString(i));
            }
            async.complete();
        });
    }

    @Test
    public void testFailedFutureRequest(TestContext context) {
        Async async = context.async();

        List<String> joins = List.of("my_joins.join_a.v1", "my_joins.join_a.v2", "my_joins.join_b.v1");
        // Set up mocks
        CompletableFuture<List<String>> futureResponse = new CompletableFuture<>();
        futureResponse.completeExceptionally(new RuntimeException("Error in KV store lookup"));

        when(mockFetcher.listJoins(anyBoolean())).thenReturn(futureResponse);

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
