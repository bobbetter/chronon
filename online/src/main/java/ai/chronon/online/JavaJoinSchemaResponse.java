package ai.chronon.online;

import ai.chronon.online.fetcher.Fetcher;

import java.util.Arrays;

public class JavaJoinSchemaResponse {
    public String joinName;
    public String keySchema;
    public String valueSchema;
    public String schemaHash;
    public ValueInfo[] valueInfos;

    public static class ValueInfo {
        public String fullName;
        public String groupName;
        public String prefix;
        public String[] leftKeys;
        public String schemaString;
        public ValueInfo(String fullName, String groupName, String prefix, String[] leftKeys, String schemaString) {
            this.fullName = fullName;
            this.groupName = groupName;
            this.prefix = prefix;
            this.leftKeys = leftKeys;
            this.schemaString = schemaString;
        }
    }

    public JavaJoinSchemaResponse(String joinName, String keySchema, String valueSchema, String schemaHash, ValueInfo[] valueInfos) {
        this.joinName = joinName;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.schemaHash = schemaHash;
        this.valueInfos = valueInfos;
    }

    public JavaJoinSchemaResponse(Fetcher.JoinSchemaResponse scalaResponse){
        this.joinName = scalaResponse.joinName();
        this.keySchema = scalaResponse.keySchema();
        this.valueSchema = scalaResponse.valueSchema();
        this.schemaHash = scalaResponse.schemaHash();
        this.valueInfos = Arrays.stream(scalaResponse.valueInfos())
                .map(v -> new ValueInfo(v.fullName(), v.groupName(), v.prefix(), v.leftKeys(), v.schemaString()))
                .toArray(ValueInfo[]::new);
    }

    public Fetcher.JoinSchemaResponse toScala() {
        return new Fetcher.JoinSchemaResponse(
                joinName,
                keySchema,
                valueSchema,
                schemaHash,
                Arrays.stream(valueInfos)
                        .map(v -> new JoinCodec.ValueInfo(v.fullName, v.groupName, v.prefix, v.leftKeys, v.schemaString))
                        .toArray(JoinCodec.ValueInfo[]::new));
    }
}
