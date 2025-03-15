#!/usr/bin/env bash

METRIC_QUERY=$(cat <<EOT
WITH
   toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS query_finish,
   toStartOfInterval(query_start_time, INTERVAL 1 MINUTE) AS query_start,
   intDiv(toUInt32(query_finish - query_start + 1),60) AS intervals,
   arrayMap( i -> ( query_start + i ), range(0, toUInt32(query_finish - query_start + 1),60) ) as timestamps
SELECT
    arrayJoin(timestamps) as t,
    normalized_query_hash,
    intDiv(sum(written_bytes),if(intervals=0,1,intervals)) as m
FROM clusterAllReplicas('{cluster}', merge(system,'^query_log'))
WHERE
    event_date = today()
    AND type!='QueryStart'
GROUP BY ALL
SETTINGS skip_unavailable_shards=1
FORMAT TSVRaw
EOT
)

clickhouse-client -q "${METRIC_QUERY}" | rare heat -m "([^\t]+)\t([^\t]+)\t([^\t]+)" -e '{timeformat {time {1}} "15:04" }' -e "{2}" -e "{floor {3}}" --rows 50 --scale log2


