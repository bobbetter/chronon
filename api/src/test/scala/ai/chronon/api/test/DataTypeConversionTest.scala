/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.api.test

import ai.chronon.api._
import ai.chronon.api.thrift.TSerializer
import ai.chronon.api.thrift.protocol.TSimpleJSONProtocol
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class DataTypeConversionTest extends AnyFlatSpec {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  it should "data type to thrift and back" in {
    // build some complex type
    val dType = StructType(
      "root",
      Array(
        StructField(
          "map",
          MapType(
            StructType("key",
                       Array(
                         StructField("a", IntType),
                         StructField("b", FloatType)
                       )),
            StructType("value", Array(StructField("c", StructType("inner", Array(StructField("d", IntType))))))
          )
        ))
    )
    val thriftType = DataType.toTDataType(dType)

    // serialize with TSimpleJson - this is what python code will do
    val jsonSerializer = new TSerializer(new TSimpleJSONProtocol.Factory())
    val json = new String(jsonSerializer.serialize(thriftType))
    logger.info(json)

    val reversedTType = ThriftJsonCodec.fromJsonStr[TDataType](json, check = true, classOf[TDataType])
    val reversed = DataType.fromTDataType(reversedTType)
    assertEquals(dType, reversed)
  }
}
