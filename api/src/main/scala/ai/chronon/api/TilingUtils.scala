package ai.chronon.api

import ai.chronon.fetcher.TileKey

import java.util
import scala.jdk.CollectionConverters._

// Convenience functions for working with tiling
object TilingUtils {

  def serializeTileKey(key: TileKey): Array[Byte] = {
    SerdeUtils.compactSerializer.get().serialize(key)
  }

  def deserializeTileKey(bytes: Array[Byte]): TileKey = {
    val key = new TileKey()
    SerdeUtils.compactDeserializer.get().deserialize(key, bytes)
    key
  }

  private def toList(arr: Array[Byte]): java.util.ArrayList[java.lang.Byte] = {
    if (arr == null) return null
    val result = new util.ArrayList[java.lang.Byte](arr.length)
    var idx = 0
    while (idx < arr.length) {
      result.add(arr(idx))
      idx += 1
    }
    result
  }

  def buildTileKey(dataset: String,
                   keyBytes: Array[Byte],
                   tileSizeMs: Option[Long],
                   tileStartTs: Option[Long]): TileKey = {
    val tileKey = new TileKey()
    tileKey.setDataset(dataset)
    tileKey.setKeyBytes(toList(keyBytes))
    tileSizeMs.foreach(tileKey.setTileSizeMillis)
    tileStartTs.foreach(tileKey.setTileStartTimestampMillis)
    tileKey
  }
}
