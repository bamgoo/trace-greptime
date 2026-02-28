package trace_greptime

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"

	"github.com/bamgoo/bamgoo"
	. "github.com/bamgoo/base"
	"github.com/bamgoo/trace"
)

type (
	greptimeDriver struct{}

	greptimeConnection struct {
		instance *trace.Instance
		client   *greptime.Client
		setting  greptimeSetting
	}

	greptimeSetting struct {
		Host     string
		Port     int
		Username string
		Password string
		Database string
		Table    string
		Timeout  time.Duration
		Insecure bool
	}
)

func init() {
	bamgoo.Register("greptime", &greptimeDriver{})
}

func (d *greptimeDriver) Connect(inst *trace.Instance) (trace.Connection, error) {
	setting := greptimeSetting{
		Host:     "127.0.0.1",
		Port:     4001,
		Database: "public",
		Table:    "traces",
		Timeout:  5 * time.Second,
		Insecure: true,
	}
	if inst != nil {
		if v, ok := getString(inst.Setting, "host"); ok && v != "" {
			setting.Host = v
		}
		if v, ok := getString(inst.Setting, "server"); ok && v != "" {
			setting.Host = v
		}
		if v, ok := getInt(inst.Setting, "port"); ok && v > 0 {
			setting.Port = v
		}
		if v, ok := getString(inst.Setting, "username"); ok {
			setting.Username = v
		}
		if v, ok := getString(inst.Setting, "user"); ok && setting.Username == "" {
			setting.Username = v
		}
		if v, ok := getString(inst.Setting, "password"); ok {
			setting.Password = v
		}
		if v, ok := getString(inst.Setting, "pass"); ok && setting.Password == "" {
			setting.Password = v
		}
		if v, ok := getString(inst.Setting, "database"); ok && v != "" {
			setting.Database = v
		}
		if v, ok := getString(inst.Setting, "db"); ok && v != "" {
			setting.Database = v
		}
		if v, ok := getString(inst.Setting, "table"); ok && v != "" {
			setting.Table = v
		}
		if v, ok := getDuration(inst.Setting, "timeout"); ok && v > 0 {
			setting.Timeout = v
		}
		if v, ok := getBool(inst.Setting, "insecure"); ok {
			setting.Insecure = v
		}
		if v, ok := getBool(inst.Setting, "tls"); ok {
			setting.Insecure = !v
		}
	}
	return &greptimeConnection{instance: inst, setting: setting}, nil
}

func (c *greptimeConnection) Open() error {
	cfg := greptime.NewConfig(c.setting.Host).
		WithPort(c.setting.Port).
		WithDatabase(c.setting.Database).
		WithAuth(c.setting.Username, c.setting.Password).
		WithInsecure(c.setting.Insecure)
	client, err := greptime.NewClient(cfg)
	if err != nil {
		return err
	}
	c.client = client
	return nil
}

func (c *greptimeConnection) Close() error { return nil }

func (c *greptimeConnection) Write(spans ...trace.Span) error {
	if c.client == nil || len(spans) == 0 {
		return nil
	}
	tbl, err := table.New(c.setting.Table)
	if err != nil {
		return err
	}
	_ = tbl.WithSanitate(false)

	fieldMap := trace.ResolveFields(c.instance.Setting, greptimeDefaultFields())
	pairs := orderedPairs(fieldMap)
	for _, p := range pairs {
		kind, dtype := greptimeFieldSpec(p.source)
		switch kind {
		case "tag":
			_ = tbl.AddTagColumn(p.target, dtype)
		case "timestamp":
			_ = tbl.AddTimestampColumn(p.target, dtype)
		default:
			_ = tbl.AddFieldColumn(p.target, dtype)
		}
	}

	for _, span := range spans {
		values := trace.SpanValues(span, c.instance.Name, c.instance.Config.Flag)
		row := make([]any, 0, len(pairs))
		for _, p := range pairs {
			row = append(row, convertGreptimeValue(p.source, values))
		}
		if err := tbl.AddRow(row...); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.setting.Timeout)
	defer cancel()
	_, err = c.client.Write(ctx, tbl)
	return err
}

type fieldPair struct {
	source string
	target string
}

func greptimeDefaultFields() map[string]string {
	return map[string]string{
		"driver":               "driver",
		"project":              "project",
		"profile":              "profile",
		"node":                 "node",
		"service":              "service",
		"service_name":         "service_name",
		"name":                 "name",
		"trace_id":             "trace_id",
		"span_id":              "span_id",
		"parent_id":            "parent_id",
		"parent_span_id":       "parent_span_id",
		"kind":                 "kind",
		"target":               "target",
		"status":               "status",
		"status_code":          "status_code",
		"status_message":       "status_message",
		"error":                "error",
		"cost_ms":              "cost_ms",
		"duration_ms":          "duration_ms",
		"start_ms":             "start_ms",
		"end_ms":               "end_ms",
		"start_time_unix_nano": "start_time_unix_nano",
		"end_time_unix_nano":   "end_time_unix_nano",
		"timestamp":            "timestamp",
		"attrs":                "attrs",
		"attributes":           "attributes",
		"resource":             "resource",
		"ts":                   "ts",
	}
}

func orderedPairs(fields map[string]string) []fieldPair {
	order := []string{
		"driver", "project", "profile", "node", "service", "service_name", "name",
		"trace_id", "span_id", "parent_id", "parent_span_id",
		"kind", "target", "status", "status_code", "status_message", "error",
		"cost_ms", "duration_ms", "start_ms", "end_ms",
		"start_time_unix_nano", "end_time_unix_nano", "timestamp",
		"attrs", "attributes", "resource", "ts",
	}
	pairs := make([]fieldPair, 0, len(fields))
	used := map[string]bool{}
	for _, source := range order {
		if target, ok := fields[source]; ok && target != "" {
			pairs = append(pairs, fieldPair{source: source, target: target})
			used[source] = true
		}
	}
	extras := make([]string, 0)
	for source, target := range fields {
		if target == "" || used[source] {
			continue
		}
		extras = append(extras, source)
	}
	sort.Strings(extras)
	for _, source := range extras {
		pairs = append(pairs, fieldPair{source: source, target: fields[source]})
	}
	return pairs
}

func greptimeFieldSpec(source string) (string, types.ColumnType) {
	switch source {
	case "driver", "project", "profile", "node", "service", "service_name", "name":
		return "tag", types.STRING
	case "ts":
		return "timestamp", types.TIMESTAMP_MILLISECOND
	case "cost_ms", "duration_ms", "start_ms", "end_ms", "start_time_unix_nano", "end_time_unix_nano", "timestamp":
		return "field", types.INT64
	default:
		return "field", types.STRING
	}
}

func convertGreptimeValue(source string, values map[string]any) any {
	v, ok := values[source]
	if !ok {
		return ""
	}
	switch source {
	case "cost_ms", "duration_ms", "start_ms", "end_ms", "start_time_unix_nano", "end_time_unix_nano", "timestamp":
		switch vv := v.(type) {
		case int64:
			return vv
		case int:
			return int64(vv)
		case float64:
			return int64(vv)
		case string:
			if n, err := strconv.ParseInt(strings.TrimSpace(vv), 10, 64); err == nil {
				return n
			}
		}
		return int64(0)
	case "attrs", "attributes", "resource":
		switch vv := v.(type) {
		case Map:
			if b, err := json.Marshal(vv); err == nil {
				return string(b)
			}
		}
		return fmt.Sprintf("%v", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func getString(m Map, key string) (string, bool) {
	if m == nil {
		return "", false
	}
	val, ok := m[key]
	if !ok {
		return "", false
	}
	v, ok := val.(string)
	return v, ok
}

func getInt(m Map, key string) (int, bool) {
	if m == nil {
		return 0, false
	}
	val, ok := m[key]
	if !ok {
		return 0, false
	}
	switch v := val.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err == nil {
			return n, true
		}
	}
	return 0, false
}

func getDuration(m Map, key string) (time.Duration, bool) {
	if m == nil {
		return 0, false
	}
	val, ok := m[key]
	if !ok {
		return 0, false
	}
	switch v := val.(type) {
	case time.Duration:
		return v, true
	case int:
		return time.Second * time.Duration(v), true
	case int64:
		return time.Second * time.Duration(v), true
	case float64:
		return time.Second * time.Duration(v), true
	case string:
		d, err := time.ParseDuration(v)
		if err == nil {
			return d, true
		}
	}
	return 0, false
}

func getBool(m Map, key string) (bool, bool) {
	if m == nil {
		return false, false
	}
	val, ok := m[key]
	if !ok {
		return false, false
	}
	v, ok := val.(bool)
	return v, ok
}
