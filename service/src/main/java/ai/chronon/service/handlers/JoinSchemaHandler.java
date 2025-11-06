package ai.chronon.service.handlers;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaJoinSchemaResponse;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class JoinSchemaHandler implements Handler<RoutingContext> {

    private final JavaFetcher fetcher;
    private static final Logger logger = LoggerFactory.getLogger(JoinSchemaHandler.class);

    public JoinSchemaHandler(JavaFetcher fetcher) {
        this.fetcher = fetcher;
    }

    @Override
    public void handle(RoutingContext ctx) {
        String entityName = ctx.pathParam("name");

        logger.debug("Retrieving join schema for {}", entityName);

        JTry<JavaJoinSchemaResponse> joinSchemaResponseTry = fetcher.fetchJoinSchema(entityName);
        if (! joinSchemaResponseTry.isSuccess()) {

            logger.error("Unable to retrieve join schema for: {}", entityName, joinSchemaResponseTry.getException());

            List<String> errorMessages = Collections.singletonList(joinSchemaResponseTry.getException().getMessage());

            ctx.response()
                    .setStatusCode(500)
                    .putHeader("content-type", "application/json")
                    .end(new JsonObject().put("errors", errorMessages).encode());
            return;
        }

        JavaJoinSchemaResponse joinSchemaResponse = joinSchemaResponseTry.getValue();

        ctx.response()
                .setStatusCode(200)
                .putHeader("content-type", "application/json")
                .end(JsonObject.mapFrom(joinSchemaResponse).encode());
    }
}
