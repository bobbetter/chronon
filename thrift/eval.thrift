namespace py gen_thrift.eval
namespace java ai.chronon.eval

enum CheckResult {
    SUCCESS = 0,
    FAILURE = 1,
    SKIPPED = 2
}

struct BaseEvalResult {
    1: optional CheckResult checkResult
    2: optional string message
}

struct JoinPartEvalResult {
    1: optional string partName
    2: optional GroupByEvalResult gbEvalResult
    3: optional BaseEvalResult keySchemaCheck
}

struct JoinEvalResult {
    1: optional BaseEvalResult leftExpressionCheck
    2: optional BaseEvalResult leftTimestampCheck
    3: optional list<JoinPartEvalResult> joinPartChecks
    4: optional BaseEvalResult derivationValidityCheck
    5: optional map<string, string> leftQuerySchema
    6: optional map<string, string> rightPartsSchema
    7: optional map<string, string> derivationsSchema
    8: optional map<string, string> externalPartsSchema
}

struct GroupByEvalResult {
    1: optional BaseEvalResult sourceExpressionCheck
    2: optional BaseEvalResult sourceTimestampCheck
    3: optional BaseEvalResult aggExpressionCheck
    4: optional BaseEvalResult derivationsExpressionCheck
    5: optional map<string, string> keySchema
    6: optional map<string, string> aggSchema
    7: optional map<string, string> derivationsSchema
}

struct StagingQueryEvalResult {
    1: optional BaseEvalResult queryCheck
    2: optional map<string, string> outputSchema
}
