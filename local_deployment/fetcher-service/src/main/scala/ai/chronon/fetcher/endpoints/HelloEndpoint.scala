// package ai.chronon.fetcher.endpoints

// import sttp.tapir._
// import sttp.tapir.json.circe._
// import io.circe.generic.auto._

// object HelloEndpoint {
  
//   // Response case class
//   case class HelloResponse(message: String, timestamp: Long)
  
//   // Define the endpoint
//   val helloEndpoint: PublicEndpoint[Unit, Unit, HelloResponse, Any] =
//     endpoint.get
//       .in("api" / "v1" / "hello")
//       .out(jsonBody[HelloResponse])
//       .description("Simple Hello World endpoint")
//       .summary("Returns a greeting message")
  
//   // Business logic for the endpoint
//   def helloLogic(unit: Unit): Either[Unit, HelloResponse] = {
//     Right(HelloResponse(
//       message = "Hello from Chronon Fetcher Service!",
//       timestamp = System.currentTimeMillis()
//     ))
//   }
// }

