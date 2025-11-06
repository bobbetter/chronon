package ai.chronon.online.connectors

import ai.chronon.api.Constants
import ai.chronon.api.ThriftJsonCodec
import ai.chronon.api.thrift.TBase
import ai.chronon.online.KVStore

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

// writes and reads thrift objects to/from the KV store - keys are strings, values are thrift objects
class ThriftKvUtil(kvStore: KVStore, table: String, waitTime: Duration = Duration.Inf) {

  def put[T <: TBase[_, _]: Manifest](prefix: String, elementKey: String, obj: T): Boolean = {
    val value: Array[Byte] = ThriftJsonCodec.toJsonStr(obj).getBytes(Constants.UTF8)
    val key = s"$prefix/$elementKey"
    val fut = kvStore.put(KVStore.PutRequest(key.getBytes, value, table))
    // await for future
    Await.result(fut, waitTime)
  }

  def get[T <: TBase[_, _]: Manifest: ClassTag](prefix: String, elementKey: String): T = {
    val key = s"$prefix/$elementKey"
    val fut = kvStore.get(KVStore.GetRequest(key.getBytes, table))
    // await for future
    val response = Await.result(fut, waitTime)
    val bytes = response.latest.get.bytes
    val str = new String(bytes, Constants.UTF8)
    ThriftJsonCodec
      .fromJsonStr[T](str, check = true, clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])
  }
}
