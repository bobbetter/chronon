package ai.chronon.api

import ai.chronon.api.thrift.protocol.TCompactProtocol
import ai.chronon.api.thrift.{TDeserializer, TSerializer}

object SerdeUtils {
  @transient
  lazy val compactSerializer: ThreadLocal[TSerializer] = new ThreadLocal[TSerializer] {
    override def initialValue(): TSerializer = new TSerializer(new TCompactProtocol.Factory())
  }

  @transient
  lazy val compactDeserializer: ThreadLocal[TDeserializer] = new ThreadLocal[TDeserializer] {
    override def initialValue(): TDeserializer = new TDeserializer(new TCompactProtocol.Factory())
  }
}
