package ai.chronon.flink.test

import ai.chronon.flink.AsyncKVStoreWriter
import ai.chronon.flink.types.AvroCodecOutput
import ai.chronon.online.Api
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar.mock

import java.util.stream.Collectors
import java.util.stream.IntStream
import scala.collection.convert.ImplicitConversions.`iterator asScala`

class AsyncKVStoreWriterTest extends AnyFlatSpec {

  private val eventTs = 1519862400075L
  private val startProcTimeTs = eventTs + 1000L

  it should "write successfully" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val requests = IntStream
      .range(0, 5)
      .mapToObj(i => new AvroCodecOutput(i.toString.getBytes, "test".getBytes, "my_dataset", eventTs, startProcTimeTs))
      .collect(Collectors.toList())
    val source: DataStream[AvroCodecOutput] = env.fromCollection(requests)

    val mockApi = mock[Api]
    val withRetries =
      AsyncKVStoreWriter
        .withUnorderedWaits(
          source,
          new MockAsyncKVStoreWriter(Seq(true), mockApi, "testFG"),
          "testFG"
        )

    val result = withRetries.executeAndCollect().toSeq
    assert(result.nonEmpty, "Expect result set to be non-empty")
    assert(result.map(_.tsMillis).forall(_ == eventTs))
    assert(result.map(_.startProcessingTime).forall(_ == startProcTimeTs))
  }

  // ensure that if we get an event that would cause the operator to throw an exception,
  // we don't crash the app
  it should "handle poison pill writes" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source: DataStream[AvroCodecOutput] = env
      .fromCollection(
        IntStream
          .range(0, 5)
          .mapToObj(i =>
            new AvroCodecOutput(i.toString.getBytes, "test".getBytes, "my_dataset", eventTs, startProcTimeTs))
          .collect(Collectors.toList())
      )

    val mockApi = mock[Api]
    val withRetries =
      AsyncKVStoreWriter
        .withUnorderedWaits(
          source,
          new MockAsyncKVStoreWriter(Seq(false), mockApi, "testFG"),
          "testFG"
        )

    val result = withRetries.executeAndCollect().toSeq
    assert(result.nonEmpty, "Expect result set to be non-empty")
    assert(result.map(_.tsMillis).forall(_ == eventTs))
    assert(result.map(_.startProcessingTime).forall(_ == startProcTimeTs))
  }

//  override def tagName: String = "asyncKVStoreWriterTest"
}
