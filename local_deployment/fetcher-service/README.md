# Chronon Fetcher Service

A modern REST API service built with **Tapir** and **Akka HTTP** for the Chronon project.

## Why This Stack?

- **Tapir**: Type-safe, declarative API definitions that generate server code, documentation, and validation automatically
- **Akka HTTP**: Battle-tested, high-performance HTTP server with excellent throughput and reactive streams
- **Circe**: Type-safe JSON with automatic codec derivation for case classes
- **Swagger UI**: Auto-generated interactive API documentation

## Project Structure

```
fetcher-service/
├── README.md                             # This file
├── src/
│   └── main/
│       ├── scala/ai/chronon/fetcher/
│       │   ├── FetcherServiceApp.scala          # Main entry point - starts HTTP server
│       │   ├── endpoints/
│       │   │   └── HelloEndpoint.scala          # Tapir endpoint definitions
│       │   └── routes/
│       │       └── Routes.scala                 # Wires Tapir to Akka HTTP
│       └── resources/
│           └── logback.xml                      # Logging configuration
```

## Application Code Overview

### 1. FetcherServiceApp.scala
Main application entry point that:
- Creates Akka ActorSystem
- Binds HTTP server to configurable host:port
- Handles graceful shutdown
- Configurable via `HOST` and `PORT` environment variables

### 2. HelloEndpoint.scala
Demonstrates Tapir's type-safe endpoint definition:
```scala
val helloEndpoint = endpoint.get
  .in("api" / "v1" / "hello")
  .out(jsonBody[HelloResponse])
  .description("Simple Hello World endpoint")
```

### 3. Routes.scala
Connects Tapir endpoints to Akka HTTP and generates Swagger UI:
- Interprets Tapir endpoints as Akka HTTP routes
- Auto-generates OpenAPI/Swagger documentation
- Combines all routes into a single handler

## Dependencies

**Scala Version**: 2.12.18

**Required Libraries**:
```
// Akka HTTP (server backend)
com.typesafe.akka::akka-http:10.2.10
com.typesafe.akka::akka-stream:2.6.21
com.typesafe.akka::akka-actor-typed:2.6.21

// Tapir (API definitions)
com.softwaremill.sttp.tapir::tapir-core:1.2.10
com.softwaremill.sttp.tapir::tapir-akka-http-server:1.2.10
com.softwaremill.sttp.tapir::tapir-json-circe:1.2.10
com.softwaremill.sttp.tapir::tapir-swagger-ui-bundle:1.2.10

// Circe (JSON)
io.circe::circe-core:0.14.5
io.circe::circe-generic:0.14.5
io.circe::circe-parser:0.14.5

// Logging
org.slf4j:slf4j-api:1.7.36
ch.qos.logback:logback-classic:1.3.15
ch.qos.logback:logback-core:1.3.15
```

## Building & Running

### Option 1: SBT

Create `build.sbt`:
```scala
name := "fetcher-service"
version := "0.1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http" % "10.2.10",
  "com.typesafe.akka" %% "akka-stream" % "2.6.21",
  "com.typesafe.akka" %% "akka-actor-typed" % "2.6.21",
  "com.softwaremill.sttp.tapir" %% "tapir-core" % "1.2.10",
  "com.softwaremill.sttp.tapir" %% "tapir-akka-http-server" % "1.2.10",
  "com.softwaremill.sttp.tapir" %% "tapir-json-circe" % "1.2.10",
  "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-bundle" % "1.2.10",
  "io.circe" %% "circe-core" % "0.14.5",
  "io.circe" %% "circe-generic" % "0.14.5",
  "io.circe" %% "circe-parser" % "0.14.5",
  "org.slf4j" % "slf4j-api" % "1.7.36",
  "ch.qos.logback" % "logback-classic" % "1.3.15"
)

mainClass in Compile := Some("ai.chronon.fetcher.FetcherServiceApp")
```

**Commands**:
```bash
sbt compile          # Compile
sbt run              # Run
sbt assembly         # Create fat JAR
```

### Option 2: Gradle

Create `build.gradle`:
```gradle
plugins {
    id 'scala'
    id 'application'
}

repositories {
    mavenCentral()
}

dependencies {
    implementation 'org.scala-lang:scala-library:2.12.18'
    implementation 'com.typesafe.akka:akka-http_2.12:10.2.10'
    implementation 'com.typesafe.akka:akka-stream_2.12:2.6.21'
    implementation 'com.typesafe.akka:akka-actor-typed_2.12:2.6.21'
    implementation 'com.softwaremill.sttp.tapir:tapir-core_2.12:1.2.10'
    implementation 'com.softwaremill.sttp.tapir:tapir-akka-http-server_2.12:1.2.10'
    implementation 'com.softwaremill.sttp.tapir:tapir-json-circe_2.12:1.2.10'
    implementation 'com.softwaremill.sttp.tapir:tapir-swagger-ui-bundle_2.12:1.2.10'
    implementation 'io.circe:circe-core_2.12:0.14.5'
    implementation 'io.circe:circe-generic_2.12:0.14.5'
    implementation 'io.circe:circe-parser_2.12:0.14.5'
    implementation 'org.slf4j:slf4j-api:1.7.36'
    implementation 'ch.qos.logback:logback-classic:1.3.15'
}

application {
    mainClass = 'ai.chronon.fetcher.FetcherServiceApp'
}
```

**Commands**:
```bash
gradle build         # Build
gradle run           # Run
```

## Configuration

Set via environment variables:
- **HOST**: Server host (default: `0.0.0.0`)
- **PORT**: Server port (default: `8080`)

Example:
```bash
HOST=localhost PORT=9000 sbt run
```

## API Endpoints

### Hello World
**GET** `/api/v1/hello`

Response:
```json
{
  "message": "Hello from Chronon Fetcher Service!",
  "timestamp": 1698087654321
}
```

### Swagger UI
**URL**: `http://localhost:8080/docs`

Interactive API documentation automatically generated from Tapir endpoint definitions.

## Testing

```bash
# Test the endpoint
curl http://localhost:8080/api/v1/hello

# View Swagger docs
open http://localhost:8080/docs
```

## Adding New Endpoints

### 1. Define the endpoint (type-safe contract)

Create `src/main/scala/ai/chronon/fetcher/endpoints/UserEndpoint.scala`:
```scala
package ai.chronon.fetcher.endpoints

import sttp.tapir._
import sttp.tapir.json.circe._
import io.circe.generic.auto._

object UserEndpoint {
  case class User(id: String, name: String, email: String)
  case class ErrorResponse(message: String)
  
  val getUserEndpoint = endpoint.get
    .in("api" / "v1" / "users" / path[String]("userId"))
    .out(jsonBody[User])
    .errorOut(jsonBody[ErrorResponse])
    .description("Get user by ID")
}
```

### 2. Wire to Akka HTTP

Update `src/main/scala/ai/chronon/fetcher/routes/Routes.scala`:
```scala
import ai.chronon.fetcher.endpoints.UserEndpoint

private val getUserRoute: Route = 
  AkkaHttpServerInterpreter().toRoute(
    UserEndpoint.getUserEndpoint.serverLogic { userId =>
      // Your business logic here
      Future.successful(
        Right(UserEndpoint.User(userId, "John Doe", "john@example.com"))
      )
    }
  )

// Add to allRoutes
val allRoutes: Route = concat(
  helloRoute,
  getUserRoute,
  swaggerRoute
)
```

### 3. Benefits
- ✅ Swagger UI automatically updated with new endpoint
- ✅ Type-safe request/response handling
- ✅ Compile-time verification
- ✅ JSON serialization handled automatically

## Architecture

### Request Flow
```
HTTP Request → Akka HTTP → Routes → Tapir Endpoint → Business Logic → JSON Response
                   ↓
              Swagger UI (auto-generated)
```

### Key Design Patterns

1. **Separation of Concerns**
   - Endpoints: API contract definitions only
   - Routes: Wiring and server logic
   - App: Server lifecycle management

2. **Type Safety**
   - Tapir ensures endpoints are type-checked at compile time
   - Invalid requests rejected automatically
   - JSON codecs derived automatically for case classes

3. **Backend Agnostic**
   - Endpoint definitions work with any backend (Akka HTTP, http4s, ZIO HTTP, etc.)
   - Easy to switch server implementations

## Extension Points

### Add Database Access
```scala
trait UserRepository {
  def findById(id: String): Future[Option[User]]
}

// In Routes
class Routes(userRepo: UserRepository)(implicit ec: ExecutionContext) {
  private val getUserRoute: Route = 
    AkkaHttpServerInterpreter().toRoute(
      UserEndpoint.getUserEndpoint.serverLogic { userId =>
        userRepo.findById(userId).map {
          case Some(user) => Right(user)
          case None => Left(ErrorResponse("Not found"))
        }
      }
    )
}
```

### Add Authentication
```scala
val secureEndpoint = endpoint
  .securityIn(auth.bearer[String]())
  .in("api" / "v1" / "secure")
  .out(jsonBody[SecureData])
```

### Add Validation
```scala
val createUserEndpoint = endpoint.post
  .in("api" / "v1" / "users")
  .in(jsonBody[CreateUserRequest])
  .out(jsonBody[User])
  .errorOut(
    oneOf[ApiError](
      oneOfVariant(statusCode(StatusCode.BadRequest).and(jsonBody[ValidationError]))
    )
  )
```

## Deployment

### Docker
Create `Dockerfile`:
```dockerfile
FROM openjdk:11-jre-slim
COPY target/scala-2.12/fetcher-service-assembly.jar /app/service.jar
EXPOSE 8080
CMD ["java", "-jar", "/app/service.jar"]
```

Build and run:
```bash
sbt assembly
docker build -t chronon-fetcher-service .
docker run -p 8080:8080 -e HOST=0.0.0.0 -e PORT=8080 chronon-fetcher-service
```

### Kubernetes
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fetcher-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: fetcher-service
  template:
    metadata:
      labels:
        app: fetcher-service
    spec:
      containers:
      - name: fetcher-service
        image: chronon-fetcher-service:latest
        ports:
        - containerPort: 8080
        env:
        - name: HOST
          value: "0.0.0.0"
        - name: PORT
          value: "8080"
```

## Next Steps

1. **Choose build tool**: Add `build.sbt` (sbt) or `build.gradle` (gradle)
2. **Build**: `sbt compile` or `gradle build`
3. **Run**: `sbt run` or `gradle run`
4. **Test**: `curl http://localhost:8080/api/v1/hello`
5. **Extend**: Add your own endpoints following the pattern above

The application code is complete and production-ready. Just add your preferred build configuration and you're good to go!
