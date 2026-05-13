package expert

import "testing"

func TestStripLeadingComments(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "plain SELECT",
			input: "SELECT 1",
			want:  "SELECT 1",
		},
		{
			name:  "multiline comment then SELECT",
			input: "-- Object Counts Audit\nSELECT * FROM system.tables",
			want:  "SELECT * FROM system.tables",
		},
		{
			name:  "flattened LLM comment SELECT on same line",
			input: "-- Object Counts Audit select * from system.tables",
			want:  "select * from system.tables",
		},
		{
			name:  "flattened LLM comment WITH on same line",
			input: "-- DDL Queue Health WITH 600 AS active_stuck_s SELECT cluster FROM system.distributed_ddl_queue",
			want:  "WITH 600 AS active_stuck_s SELECT cluster FROM system.distributed_ddl_queue",
		},
		{
			name:  "block comment then SELECT",
			input: "/* some comment */ SELECT 1",
			want:  "SELECT 1",
		},
		{
			name:  "multiple line comments",
			input: "-- line 1\n-- line 2\nSELECT 1",
			want:  "SELECT 1",
		},
		{
			name:  "only comment no SQL",
			input: "-- just a comment",
			want:  "",
		},
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
		{
			name:  "whitespace only",
			input: "   \n\t  ",
			want:  "",
		},
		{
			name:  "SHOW after comment",
			input: "-- show tables\nSHOW TABLES",
			want:  "SHOW TABLES",
		},
		{
			name:  "EXISTS after comment",
			input: "-- check existence\nEXISTS TABLE foo",
			want:  "EXISTS TABLE foo",
		},
		{
			name:  "DESCRIBE after comment",
			input: "-- describe table\nDESCRIBE system.tables",
			want:  "DESCRIBE system.tables",
		},
		{
			name:  "real log case: Resource Utilization",
			input: "-- Resource Utilization Memory Usage select 'Memory Usage' AS resource, formatReadableSize(used_ram) AS used FROM system.tables",
			want:  "select 'Memory Usage' AS resource, formatReadableSize(used_ram) AS used FROM system.tables",
		},
		{
			name:  "real log case: Disk Health",
			input: "-- Disk Health select name as disk, path from system.disks where lower(type) = 'local'",
			want:  "select name as disk, path from system.disks where lower(type) = 'local'",
		},
		{
			name:  "real log case: DDL Queue with WITH",
			input: "-- DDL Queue Health WITH 600 AS active_stuck_s, 100 AS backlog_warn SELECT cluster, countIf(status != 'Finished') AS not_finished FROM system.distributed_ddl_queue GROUP BY cluster",
			want:  "WITH 600 AS active_stuck_s, 100 AS backlog_warn SELECT cluster, countIf(status != 'Finished') AS not_finished FROM system.distributed_ddl_queue GROUP BY cluster",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripLeadingComments(tt.input)
			if got != tt.want {
				t.Errorf("stripLeadingComments(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
