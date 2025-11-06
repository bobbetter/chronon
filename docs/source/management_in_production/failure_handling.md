# Failure Handling

The most critical failures that can occur in Chronon are usually in the online production flow, because that can affect live inference.

Failures in offline batch data generation are usually less urgent, however they can also cause issues if you have things like model training or batch inference pipelines downstream.


## The Online Path

The below sections outline the various failure scenarios and their impacts.

### Group By Batch Uploads

Batch upload datasets are computed daily and pushed to the KV store. This data is used to serve features either fully (in case of batch GroupBys) or partially along with streaming data (written by a Flink job) in case of GroupBys with streaming enabled.

Only the most recent run of a GroupBy batch upload is relevant -- these jobs are designed to overwrite the snapshot for the entire keyset on each upload.

The list below outlines the effect of the GroupBy upload job failing to run for various durations. Failures can be caused either by missing upstream data (the source partition source never triggers), or by a failure internal to the computation or upload.

* Short upstream data incidents (data lands within `2d`): No meaningful impact on data or system performance. For a batch GroupBy, online data will only be accurate as of the last upload, but for streaming GroupBys, if the streaming continues uninterrupted then online data should also be correct.
* Longer upstream data incidents
  *  Data lands > `2d` but within `GROUPBY_STREAMING_TTL`
     * Batch GroupBys: Data is stale, accurate as of the last upload
     * Streaming GroupBys: Data is correct, but could experience slightly degraded latencies due to an increasing number of streaming tiles. Likely unproblematic except for extremely latency sensitive use cases.
  * Data lands > `GROUPBY_STREAMING_TTL` but within `GROUP_BY_BATCH_TTL`
     * Batch GroupBys: Data is stale, accurate as of the last upload
     * Streaming GroupBys: Data is partially incorrect due to overcounting the tail of the window and GC deleting some streaming events.
  * Data does not land within GROUP_BY_BATCH_TTL
    * Batch GroupBys: No data until manual mitigation is run
    * Streaming GroupBys: No data until manual mitigation is run
    * Note: Manual mitigation (see below) can be run anytime prior to the `GROUP_BY_BATCH_TTL` cutoff to reset the TTL countdown.


`GROUPBY_STREAMING_TTL` and `GROUP_BY_BATCH_TTL` can be configured in various ways, depending on your KV store implementation, but we suggest setting those values with the above considerations in mind.


### Manual Mitigation

Chronon operators can use the normal `--mode upload` CLI to publish an older batch dataset as the latest. This could be used to re-issue the last “good” run as the version of choice over a newer version in case of bad data, or if a failure is persisting for many days and a customer wants to extend the time that they have for resolution before hitting TTL limits.


### Enforcing Data Quality (future work)

A potential improvement to Chronon would be to allow users some way to set rules about the data generated during batch computation for upload. I.e. a maximum null rate for a particular feature, row count, etc.

Enforcement of these rules could be configured to sit between the computation stage and the upload stage during orchestration. Failures would alert the user and block upload. In these instances, Chronon would continue to serve the most recent data that passed the data quality checks.

### Alerting

* If self hosting: It's strongly advised to set alerts within your orchestrator for failures and SLA misses on batch upload jobs.
* If using Zipline: Zipline Hub orchestrator handles failure alerting, and can be integrated with internal alerting/pager systems.


### Partial failures

By default, Chronon treats each GroupBy as its own unit of computation. What this means is that if a Join uses many different GroupBys, and one of them experiences a failure scenario (as outlined above), then only the data for those features within the failing GroupBy are affected.

In essence, this means "best-effort" feature serving when consumers make a fetch request for a Chronon Join.

## Streaming Failures

Failure in the streaming job will result in a degradation of data freshness (see [Overview](./Overview.md)).



## Incorrect Data Scenarios

Due to an upstream data incident a customer might end up writing out incorrect GroupBy upload data for the latest day. 

Batch GroupBys would serve incorrect data until manual mitigation is run, or the next day’s run succeeds (assuming the data is fixed).

Streaming GroupBys would serve incorrect data until manual mitigation is run, or the next day’s run succeeds (assuming the data is fixed). Incorrect streaming data might also persist until the next successful upload.

See [Enforcing Data Quality](#enforcing-data-quality) for future plans.
