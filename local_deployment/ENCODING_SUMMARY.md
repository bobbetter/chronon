# Chronon Data Encoding Summary

## Overview

Chronon uses **two encoding formats** for different purposes:

### Terminology

- **Avro Binary format** — The encoding protocol (compact, schema-aware serialization)
- **Avro-encoded bytes** — The output: a `byte[]` array produced by Avro Binary encoding
- **Thrift** — IDL for defining metadata structures; can serialize to JSON or binary

| What | Format | Purpose |
|------|--------|---------|
| **Metadata / Configuration** | Thrift | Schema definitions, GroupBy/Join configs |
| **Feature Data / Values** | Avro | Actual computed feature values |

---

## Thrift — Configuration Layer

### What It Encodes

- GroupBy, Join, Aggregation, MetaData, Source, Query definitions
- GroupByServingInfo, JoinServingInfo
- Window, Operation enums

### Schema Definition

Thrift is an **Interface Definition Language (IDL)** similar to Protobuf. Schemas are defined in `.thrift` files:

```thrift
// From thrift/api.thrift
struct GroupBy {
    1: optional MetaData metaData
    2: optional list<Source> sources
    3: optional list<string> keyColumns
    4: optional list<Aggregation> aggregations
    5: optional Accuracy accuracy
    6: optional string backfillStartDate
}
```

### Serialization Formats

Thrift objects can be serialized in multiple formats:

| Protocol | Format | Use Case |
|----------|--------|----------|
| `TSimpleJSONProtocol` | Human-readable JSON | Config files (e.g., `logins.v1__1`) |
| `TCompactProtocol` | Binary | Compact storage/network |
| `TBinaryProtocol` | Binary | Fast serialization |

### Thrift vs Protobuf

| Aspect | Thrift | Protobuf |
|--------|--------|----------|
| **Developer** | Facebook (2007) | Google (2008) |
| **RPC Built-in** | ✅ Yes | ❌ Needs gRPC |
| **Native JSON** | ✅ `TSimpleJSONProtocol` | ⚠️ Requires extra library |
| **Union Types** | ✅ Native `union` | ⚠️ `oneof` (proto3) |
| **Ecosystem** | Smaller | Larger |

### Why Chronon Uses Thrift

1. **Native JSON support** — Config files are human-readable without extra libraries
2. **Union types** — Clean modeling of polymorphic types like `Source`
3. **Code generation** — Auto-generates Java/Python classes from `.thrift` files

---

## Avro — Feature Data Layer

Chronon uses **Avro Binary format** to encode feature data into bytes for compact storage and fast serving.

### What It Encodes

- `key_bytes`: Key fields (e.g., `user_id`) encoded as Avro
- `value_bytes`: Aggregation results (counts, LAST_K arrays, etc.) encoded as Avro

### Encoding Example

`user_18` encoded using Avro Binary format:

| Byte | Value (Hex) | Value (Dec) | Meaning |
|------|-------------|-------------|---------|
| 0 | `\x02` | 2 | Avro union index (non-null) |
| 1 | `\x0e` | 14 | String length (7 × 2 zigzag) |
| 2-8 | `user_18` | 117,115,101,114,95,49,56 | ASCII characters |

### Why Avro for Feature Data

| Concern | Thrift | Avro |
|---------|--------|------|
| **Volume** | Low (one config per GroupBy) | High (millions of rows) |
| **Schema source** | `.thrift` IDL files | Derived from config at runtime |
| **Human editing** | ✅ Yes (JSON configs) | ❌ No (binary only) |
| **Dynamic schemas** | ❌ Fixed at compile time | ✅ Schema per GroupBy |

---

## How They Work Together

```
┌────────────────────────────────────────────────────────────────┐
│                        THRIFT                                   │
│  GroupBy config defines:                                        │
│    - keys: ["user_id"]                                         │
│    - aggregations: COUNT, LAST_K                               │
│    - windows: 3d, 14d, 30d                                     │
└───────────────────────────┬────────────────────────────────────┘
                            │ generates Avro schema
                            ▼
┌────────────────────────────────────────────────────────────────┐
│                         AVRO                                    │
│  Key Schema:   { "user_id": "string" }                         │
│  Value Schema: { "count_3d": "long", "last_k_14d": [...] }     │
│                                                                 │
│  Serialized data:                                               │
│    key_bytes:   \x02\x0euser_18                                │
│    value_bytes: \x02\x02\x04\x0e\x64...                        │
└────────────────────────────────────────────────────────────────┘
```

**Thrift defines how to interpret the data. Avro Binary format encodes the actual feature values into bytes.**

---

## Data Flow

```
GroupBy Config → Spark Backfill → Parquet (upload table) → DynamoDB (KV store)
     (Thrift)                     (Avro-encoded bytes)    (Avro-encoded bytes)
```

## Storage Locations

| Stage | Location | Format |
|-------|----------|--------|
| Config | `app/compiled/group_bys/{team}/{name}.v{version}` | Thrift as JSON |
| Upload Table | `metastore/warehouse/{namespace}.db/{name}_upload/` | Parquet with Avro-encoded bytes |
| KV Store | DynamoDB table `{NAME}_BATCH` | Avro-encoded bytes |

## Schema Metadata

Schema is stored alongside data with a special key (`__group_by_serving_info__`) containing `GroupByServingInfo` (Thrift object serialized as JSON). This enables the fetcher to decode the Avro-encoded bytes at serving time.

---

## Key Source Files

**Thrift definitions:**
- `thrift/api.thrift` — GroupBy, Join, Aggregation, MetaData structs
- `thrift/fetcher.thrift` — GroupByServingInfo, JoinServingInfo
- `api/src/main/scala/ai/chronon/api/ThriftJsonCodec.scala` — Thrift serialization

**Avro encoding:**
- `online/src/main/scala/ai/chronon/online/serde/AvroCodec.scala` — Avro Binary format encoding/decoding
- `online/src/main/scala/ai/chronon/online/serde/AvroConversions.scala` — Schema conversion
- `spark/src/main/scala/ai/chronon/spark/KvRdd.scala` — Key/value byte generation
