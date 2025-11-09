package tui

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/rs/zerolog/log"
)

// Flamegraph query templates for different category types
const flamegraphQueryByHash = `
SELECT
	count() AS samples,
	concat(
		multiIf(
			position( toString(trace_type), 'Memory') > 0 AND sum(size) >= 0, 'allocate;',
			position( toString(trace_type), 'Memory') > 0 AND sum(size) < 0, 'free;',
			concat( toString(trace_type), ';')
		),
		arrayStringConcat(arrayReverse(arrayMap(x -> concat( demangle(addressToSymbol(x)), '#', addressToLine(x) ), trace)), ';')
	) AS stack
FROM clusterAllReplicas('%s', merge(system, '^trace_log'))
WHERE query_id IN (
    SELECT query_id
    FROM clusterAllReplicas('%s', merge(system, '^query_log'))
    WHERE normalized_query_hash = '%s'
    AND event_date >= toDate('%s') AND event_date <= toDate('%s')
    AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s')
)
AND trace_type = '%s'
GROUP BY trace, trace_type
SETTINGS allow_introspection_functions=1
`

const flamegraphQueryByTable = `
SELECT
	count() AS samples,
	concat(
		multiIf(
			position( toString(trace_type), 'Memory') > 0 AND sum(size) >= 0, 'allocate;',
			position( toString(trace_type), 'Memory') > 0 AND sum(size) < 0, 'free;',
			concat( toString(trace_type), ';')
		),
		arrayStringConcat(arrayReverse(arrayMap(x -> concat( demangle(addressToSymbol(x)), '#', addressToLine(x) ), trace)), ';')
	) AS stack
FROM clusterAllReplicas('%s', merge(system, '^trace_log'))
WHERE query_id IN (
    SELECT query_id
    FROM clusterAllReplicas('%s', merge(system, '^query_log'))
    WHERE hasAll(tables, ['%s'])
    AND event_date >= toDate('%s') AND event_date <= toDate('%s')
    AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s')
)
AND trace_type = '%s'
GROUP BY trace, trace_type
SETTINGS allow_introspection_functions=1
`

const flamegraphQueryByHost = `
SELECT
	count() AS samples,
	concat(
		multiIf(
			position( toString(trace_type), 'Memory') > 0 AND sum(size) >= 0, 'allocate;',
			position( toString(trace_type), 'Memory') > 0 AND sum(size) < 0, 'free;',
			concat( toString(trace_type), ';')
		),
		arrayStringConcat(arrayReverse(arrayMap(x -> concat( demangle(addressToSymbol(x)), '#', addressToLine(x) ), trace)), ';')
	) AS stack
FROM clusterAllReplicas('%s', merge(system, '^trace_log'))
WHERE query_id IN (
    SELECT query_id
    FROM clusterAllReplicas('%s', merge(system, '^query_log'))
    WHERE hostName() = '%s'
    AND event_date >= toDate('%s') AND event_date <= toDate('%s')
    AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s')
)
AND trace_type = '%s'
GROUP BY trace, trace_type
SETTINGS allow_introspection_functions=1
`

const flamegraphQueryByError = `
SELECT
	count() AS samples,
	concat(
		multiIf(
			position( toString(trace_type), 'Memory') > 0 AND sum(size) >= 0, 'allocate;',
			position( toString(trace_type), 'Memory') > 0 AND sum(size) < 0, 'free;',
			concat( toString(trace_type), ';')
		),
		arrayStringConcat(arrayReverse(arrayMap(x -> concat( demangle(addressToSymbol(x)), '#', addressToLine(x) ), trace)), ';')
	) AS stack
FROM clusterAllReplicas('%s', merge(system, '^trace_log'))
WHERE query_id IN (
    SELECT query_id
    FROM clusterAllReplicas('%s', merge(system, '^query_log'))
    WHERE normalized_query_hash = '%s'
    AND event_date >= toDate('%s') AND event_date <= toDate('%s')
    AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s')
)
AND trace_type = '%s'
GROUP BY trace, trace_type
SETTINGS allow_introspection_functions=1
`

const flamegraphQueryByTimeRange = `
SELECT
	count() AS samples,
	concat(
		multiIf(
			position( toString(trace_type), 'Memory') > 0 AND sum(size) >= 0, 'allocate;',
			position( toString(trace_type), 'Memory') > 0 AND sum(size) < 0, 'free;',
			concat( toString(trace_type), ';')
		),
		arrayStringConcat(arrayReverse(arrayMap(x -> concat( demangle(addressToSymbol(x)), '#', addressToLine(x) ), trace)), ';')
	) AS stack
FROM clusterAllReplicas('%s', merge(system, '^trace_log'))
WHERE query_id IN (
    SELECT query_id
    FROM clusterAllReplicas('%s', merge(system, '^query_log'))
    WHERE event_date >= toDate('%s') AND event_date <= toDate('%s')
    AND event_time >= parseDateTimeBestEffort('%s') AND event_time <= parseDateTimeBestEffort('%s')
)
AND trace_type = '%s'
GROUP BY trace, trace_type
SETTINGS allow_introspection_functions=1
`

// getFlamegraphQuery returns the appropriate query string based on category type
func (a *App) getFlamegraphQuery(categoryType CategoryType, categoryValue string, traceType TraceType,
	fromDateStr, toDateStr, fromStr, toStr, cluster string) string {
	switch categoryType {
	case CategoryQueryHash:
		return fmt.Sprintf(flamegraphQueryByHash, cluster, cluster, categoryValue, fromDateStr, toDateStr, fromStr, toStr, traceType)
	case CategoryTable:
		return fmt.Sprintf(flamegraphQueryByTable, cluster, cluster, categoryValue, fromDateStr, toDateStr, fromStr, toStr, traceType)
	case CategoryHost:
		return fmt.Sprintf(flamegraphQueryByHost, cluster, cluster, categoryValue, fromDateStr, toDateStr, fromStr, toStr, traceType)
	case CategoryError:
		parts := strings.Split(categoryValue, ":")
		if len(parts) != 2 {
			return ""
		}
		return fmt.Sprintf(flamegraphQueryByError, cluster, cluster, parts[1], fromDateStr, toDateStr, fromStr, toStr, traceType)
	default:
		// If categoryType is not specified, use only time range
		return fmt.Sprintf(flamegraphQueryByTimeRange, cluster, cluster, fromDateStr, toDateStr, fromStr, toStr, traceType)
	}
}

// ShowFlamegraphViewer displays the flamegraph with the given parameters
func (a *App) ShowFlamegraphViewer(categoryType CategoryType, categoryValue string, traceType TraceType, fromTime, toTime time.Time, cluster string) tea.Cmd {
	viewer := newFlamegraphViewer(a.width, a.height)
	viewer.categoryType = categoryType
	viewer.categoryValue = categoryValue
	viewer.traceType = traceType

	a.flamegraphHandler = viewer
	a.currentPage = pageFlamegraph

	// Start fetching data
	return a.fetchFlamegraphDataCmd(categoryType, categoryValue, traceType, fromTime, toTime, cluster)
}

// fetchFlamegraphDataCmd fetches flamegraph data from ClickHouse
func (a *App) fetchFlamegraphDataCmd(categoryType CategoryType, categoryValue string, traceType TraceType, fromTime, toTime time.Time, cluster string) tea.Cmd {
	return func() tea.Msg {
		// Format dates for the query
		fromStr := fromTime.Format("2006-01-02 15:04:05 -07:00")
		toStr := toTime.Format("2006-01-02 15:04:05 -07:00")
		fromDateStr := fromTime.Format("2006-01-02")
		toDateStr := toTime.Format("2006-01-02")

		query := a.getFlamegraphQuery(categoryType, categoryValue, traceType, fromDateStr, toDateStr, fromStr, toStr, cluster)

		rows, queryErr := a.state.ClickHouse.Query(query)
		if queryErr != nil {
			return FlamegraphDataMsg{Err: fmt.Errorf("error querying ClickHouse: %v", queryErr)}
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("can't close flamegraph query rows")
			}
		}()

		// Build flamegraph tree from rows
		root, maxDepth, maxCount, err := buildFlamegraphFromRows(rows)
		if err != nil {
			return FlamegraphDataMsg{Err: fmt.Errorf("error building flamegraph: %v", err)}
		}

		return FlamegraphDataMsg{
			Root:     root,
			MaxDepth: maxDepth,
			MaxCount: maxCount,
		}
	}
}

// ShowFlamegraphFormWrapper is a wrapper that uses the bubbletea config form and then launches the viewer
func (a *App) ShowFlamegraphFormWrapper(categoryType CategoryType, categoryValue string, traceType TraceType, fromTime, toTime time.Time, cluster string) {
	// Always use the bubbletea native viewer
	a.ShowFlamegraphViewer(categoryType, categoryValue, traceType, fromTime, toTime, cluster)
}

// Note: Query templates are defined in flamegraph_handler.go
