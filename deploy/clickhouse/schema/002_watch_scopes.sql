CREATE TABLE IF NOT EXISTS watch_scopes
(
    ts         DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
    cluster_id LowCardinality(String),
    api_group  LowCardinality(String),
    api_version LowCardinality(String),
    kind       LowCardinality(String),
    namespace  String,                     -- '' = cluster-scoped or all-namespaces
    action     LowCardinality(String),     -- Started|Stopped
    rule_ref   String                      -- "<kind>/<namespace>/<name>", e.g. "streamrule/demo/audit" or "clusterstreamrule//platform"
)
ENGINE = MergeTree
ORDER BY (cluster_id, api_group, kind, namespace, ts);
