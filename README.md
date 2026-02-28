# trace-greptime

bamgoo greptime trace driver.

```toml
[trace.greptime]
driver = "greptime"
fields = { trace_id = "tid", span_id = "sid", parent_span_id = "psid", timestamp = "timestamp" }
[trace.greptime.setting]
host = "127.0.0.1"
port = 4001
database = "public"
table = "traces"
```

`fields` is configured on `[trace.greptime]` (not under `setting`).
