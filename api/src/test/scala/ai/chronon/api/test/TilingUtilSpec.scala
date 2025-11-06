package ai.chronon.api.test

import ai.chronon.api.TilingUtils
import ai.chronon.fetcher.TileKey
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

class TilingUtilSpec extends AnyFlatSpec with Matchers {

  "TilingUtils" should "serialize and deserialize TileKey" in {
    val key = new TileKey()
    key.setDataset("MY_GROUPBY_V1_STREAMING")
    key.setKeyBytes("key".getBytes.toList.asJava.asInstanceOf[java.util.List[java.lang.Byte]])
    key.setTileSizeMillis(10.hours.toMillis)
    key.setTileStartTimestampMillis(1738195200000L) // 2025-01-29T00:00:00Z
    val bytes = TilingUtils.serializeTileKey(key)
    val deserializedKey = TilingUtils.deserializeTileKey(bytes)
    deserializedKey.getDataset should be("MY_GROUPBY_V1_STREAMING")
    deserializedKey.getKeyBytes.asScala.map(_.toByte).toArray should be("key".getBytes)
    deserializedKey.getTileSizeMillis should be(10.hours.toMillis)
    deserializedKey.getTileStartTimestampMillis should be(1738195200000L)
  }
}
