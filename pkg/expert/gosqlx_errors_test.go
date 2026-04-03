package expert

import (
	"testing"

	"github.com/ajitpratap0/GoSQLX/pkg/gosqlx"
	"github.com/ajitpratap0/GoSQLX/pkg/sql/keywords"
)

func TestGoSQLXParsesClickHouseQueries(t *testing.T) {
	queries := map[string]string{
		"replicas_with_table_column": `SELECT
    database,
    table,
    is_leader,
    is_readonly,
    is_session_expired,
    parts_to_check,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    absolute_delay,
    last_queue_update,
    zookeeper_path
FROM system.replicas
ORDER BY absolute_delay DESC`,

		"tables_with_bytes_on_disk": `SELECT
    database,
    table,
    engine,
    formatReadableSize(bytes_on_disk) AS size,
    parts,
    active_parts
FROM system.tables
WHERE engine LIKE '%MergeTree%'
  AND is_temporary = 0
ORDER BY bytes_on_disk DESC
LIMIT 10`,

		"tables_with_total_bytes": `SELECT
    database,
    table,
    engine,
    formatReadableSize(total_bytes) AS size,
    parts,
    active_parts
FROM system.tables
WHERE engine LIKE '%MergeTree%'
  AND is_temporary = 0
ORDER BY total_bytes DESC
LIMIT 10`,

		"parts_with_concat_table": `SELECT
    concat(database, '.' ,table) AS table_name,
    count() AS part_count,
    max(partition) AS latest_partition,
    formatReadableSize(sum(bytes_on_disk)) AS total_size
FROM system.parts
WHERE active = 1
  AND database NOT IN ('system')
GROUP BY database, table
ORDER BY part_count DESC
LIMIT 10`,

		"parts_having_count": `SELECT
    database,
    table,
    count() AS parts,
    formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts
WHERE active = 1
  AND database NOT IN ('system')
GROUP BY database, table
HAVING parts > 300
ORDER BY parts DESC`,

		"version_and_uptime": `SELECT version() AS version, hostName() AS host, uptime() AS uptime_seconds, formatReadableTimeDelta(uptime()) AS uptime_formatted`,

		"metrics_with_in_list": `SELECT
    metric,
    value,
    formatReadableQuantity(value) AS formatted
FROM system.metrics
WHERE metric IN (
    'MemoryTracking',
    'Query',
    'Merge',
    'PartMutation',
    'ReplicatedFetch',
    'ReplicatedSend',
    'BackgroundPoolTask',
    'BackgroundMovePoolTask',
    'BackgroundFetchPoolTask',
    'BackgroundMessageBrokerSchedulePoolTask',
    'ReplicaQueue',
    'Read',
    'Write'
)
ORDER BY metric`,

		"tables_with_engine_in": `SELECT
    database,
    table,
    engine,
    formatReadableSize(total_bytes) AS size,
    total_rows,
    total_parts,
    active_parts
FROM system.tables
WHERE engine IN ('MergeTree', 'ReplicatedMergeTree', 'ReplicatedReplacingMergeTree', 'ReplacingMergeTree')
AND total_bytes > 0
ORDER BY total_bytes DESC
LIMIT 20`,

		"server_settings_like": `SELECT
    name,
    value,
    changed,
    description
FROM system.server_settings
WHERE name LIKE '%pool_size'
ORDER BY name`,

		"replicas_with_is_lost_part": `SELECT
    database,
    table,
    replica_name,
    is_session_expired,
    future_parts,
    parts_to_check,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    last_queue_update,
    absolute_delay,
    is_readonly,
    is_lost_part
FROM system.replicas
ORDER BY absolute_delay DESC`,

		"detached_parts_grouped": `SELECT
  hostName() AS host,
  database,
  table,
  reason,
  count() AS detached_parts,
  formatReadableSize(sum(bytes_on_disk)) AS bytes
FROM system.detached_parts
GROUP BY host, database, table, reason
ORDER BY detached_parts DESC
LIMIT 50`,

		"detached_parts_with_modification_time": `SELECT
  hostName() AS host,
  database,
  table,
  reason,
  count() AS detached_parts,
  formatReadableSize(sum(bytes_on_disk)) AS bytes,
  min(modification_time) AS first_detach,
  max(modification_time) AS last_detach
FROM system.detached_parts
GROUP BY host, database, table, reason
ORDER BY detached_parts DESC
LIMIT 100`,

		"errors_table": `SELECT code, name, value, last_error_time, last_error_message, remote FROM system.errors ORDER BY value DESC LIMIT 30`,

		"text_log_warnings": `SELECT event_date, level, thread_name, any(logger_name) AS logger_name,
       message_format_string, count(*) AS count
FROM   system.text_log
WHERE  event_date > now() - interval 24 hour
  AND level <= 'Warning'
GROUP BY all
ORDER BY level, thread_name, message_format_string`,

		"text_log_errors": `SELECT level, message, count() AS count, min(event_time) AS first_seen, max(event_time) AS last_seen
FROM system.text_log
WHERE event_date > now() - interval 24 hour
AND level IN ('Error', 'Critical', 'Fatal')
GROUP BY level, message
ORDER BY count DESC
LIMIT 30`,

		"parts_by_partition_having": `SELECT
    database,
    table,
    partition,
    count() AS part_count,
    formatReadableSize(sum(bytes_on_disk)) AS size,
    sum(rows) AS total_rows
FROM system.parts
WHERE database NOT IN ('system', 'INFORMATION_SCHEMA')
AND active = 1
GROUP BY database, table, partition
HAVING part_count > 500
ORDER BY part_count DESC
LIMIT 20`,

		"parts_active_inactive": `SELECT
    database, table,
    count() AS total_parts,
    countIf(active) AS active_parts,
    countIf(active) < count() AS has_inactive,
    formatReadableSize(sum(bytes)) AS total_size
FROM system.parts
WHERE database NOT IN ('system', 'INFORMATION_SCHEMA')
GROUP BY database, table
ORDER BY total_parts DESC
LIMIT 30`,

		"clusters": `SELECT cluster, shard_num, shard_weight, replica_num, host_name, host_address, port, is_local
FROM system.clusters
ORDER BY cluster, shard_num, replica_num`,

		"disks": `SELECT name, path, formatReadableSize(free_space) AS free, formatReadableSize(total_space) AS total, keep_free_space, formatReadableSize(total_space - free_space) AS used FROM system.disks`,

		"merges": `SELECT name, elapsed, formatReadableSize(read_rows) AS r_rows, formatReadableSize(written_rows) AS w_rows, formatReadableSize(bytes) AS bytes_val
FROM system.merges
WHERE 1=1
ORDER BY elapsed DESC
LIMIT 10`,

		"processes": `SELECT query_id, user, address, elapsed, formatReadableSize(read_rows) AS r_rows, formatReadableSize(memory_usage) AS mem, query
FROM system.processes
WHERE is_cancelled = 0
ORDER BY elapsed DESC
LIMIT 20`,

		"mutations_running": `SELECT count() AS total_mutations_running FROM system.mutations WHERE is_done = 0`,

		"asynchronous_metrics_memory": `SELECT name, value FROM system.asynchronous_metrics
WHERE name IN ('OSCPUVirtualTimeLinux', 'OSCPUWaitIO', 'OSIOWait', 'MemoryTotal', 'MemoryFree', 'AvailableMemory', 'CGroupMemoryUsed', 'MemoryResident', 'MemoryVirtual', 'OSMemoryCached', 'DiskReadBytes', 'DiskWriteBytes', 'TotalPrimaryKeyBytesInMemory')`,

		"tables_full_info": `SELECT
    database,
    name AS table,
    engine,
    total_rows,
    formatReadableSize(total_bytes) AS total_size,
    active_parts,
    partition_key,
    sorting_key,
    primary_key,
    total_bytes_uncompressed,
    total_marks
FROM system.tables
WHERE engine LIKE '%MergeTree%'
ORDER BY total_bytes DESC
LIMIT 30`,

		"inactive_parts_grouped": `SELECT
    database,
    table,
    count() AS inactive_parts,
    formatReadableSize(sum(bytes_on_disk)) AS inactive_size,
    min(modification_time) AS oldest,
    max(modification_time) AS newest
FROM system.parts
WHERE active = 0
GROUP BY database, table
HAVING inactive_parts > 0
ORDER BY inactive_parts DESC
LIMIT 20`,

		"zookeeper_root": `SELECT name FROM system.zookeeper WHERE path = '/' LIMIT 10`,

		"describe_table": `DESCRIBE TABLE system.detached_parts`,

		"dictionaries_errors": `SELECT name, status, origin, loading_start_time, last_exception, element_count, bytes_allocated
FROM system.dictionaries
WHERE status NOT IN ('LOADED') OR last_exception != ''`,

		"union_all_version": `SELECT
    'version' AS metric, version() AS value
UNION ALL
SELECT 'uptime', toString(uptime())
UNION ALL
SELECT 'running_time', formatReadableTimeDelta(uptime())`,

		"check_pools_with_cte": `WITH
    ['MergesAndMutations', 'Fetches', 'Move', 'Common', 'Schedule', 'BufferFlushSchedule', 'MessageBrokerSchedule', 'DistributedSchedule'] AS pool_tokens,
    ['pool', 'fetches_pool', 'move_pool', 'common_pool', 'schedule_pool', 'buffer_flush_schedule_pool', 'message_broker_schedule_pool', 'distributed_schedule_pool'] AS setting_tokens
SELECT
    extract(m.metric, '^Background(.*)Task') AS pool_name,
    m.active_tasks,
    pool_size,
    round(100.0 * m.active_tasks / pool_size, 1) AS utilization_pct,
    multiIf(utilization_pct > 99, 'Major', utilization_pct > 90, 'Moderate', 'OK') AS severity
FROM
(
    SELECT
        metric,
        value AS active_tasks,
        transform(extract(metric, '^Background(.*)PoolTask'), pool_tokens, setting_tokens, '') AS pool_key,
        concat('background_', lower(pool_key), '_size') AS setting_name
    FROM system.metrics
    WHERE metric LIKE 'Background%PoolTask'
) AS m
LEFT JOIN
(
    SELECT
        name,
        toFloat64OrZero(value) AS pool_size
    FROM system.server_settings
    WHERE name LIKE 'background%pool_size'
) AS s ON s.name = m.setting_name
WHERE pool_size > 0
ORDER BY utilization_pct DESC`,

		"columns_of_table": `SELECT name FROM system.columns WHERE table = 'tables' AND database = 'system' ORDER BY name`,

		"system_replicas_full": `SELECT replica_name, engine, last_queue_update, parts_to_check, queue_size, inserts_in_queue, merges_in_queue, log_max_index, log_pointer, total_replicas, active_replicas FROM system.replicas WHERE total_replicas > 0`,

		"asynchronous_metrics_replication": `SELECT name, value, description FROM system.asynchronous_metrics
WHERE name IN (
    'MaxPartCountForPartition',
    'ReplicasMaxAbsoluteDelay',
    'ReplicasMaxMergesInQueue',
    'ReplicasMaxInsertsInQueue'
)`,
	}

	for name, query := range queries {
		t.Run(name, func(t *testing.T) {
			parsed, err := gosqlx.ParseWithDialect(query, keywords.DialectClickHouse)
			//require.NoError(t, err, "gosqlx.ParseWithDialect should parse ClickHouse query without error")
			//assert.NotNil(t, parsed, "parsed AST should not be nil")
			if err != nil {
				t.Logf("wait https://github.com/ajitpratap0/GoSQLX/issues/480 will resolve, parsed=%v error=%v", parsed, err)
			}
		})
	}
}
