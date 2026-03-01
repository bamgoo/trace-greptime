# trace-greptime

`trace-greptime` 是 `trace` 模块的 `greptime` 驱动。

## 安装

```bash
go get github.com/infrago/trace@latest
go get github.com/infrago/trace-greptime@latest
```

## 接入

```go
import (
    _ "github.com/infrago/trace"
    _ "github.com/infrago/trace-greptime"
    "github.com/infrago/infra"
)

func main() {
    infra.Run()
}
```

## 配置示例

```toml
[trace]
driver = "greptime"
```

## 公开 API（摘自源码）

- `func (d *greptimeDriver) Connect(inst *trace.Instance) (trace.Connection, error)`
- `func (c *greptimeConnection) Open() error`
- `func (c *greptimeConnection) Close() error { return nil }`
- `func (c *greptimeConnection) Write(spans ...trace.Span) error`

## 排错

- driver 未生效：确认模块段 `driver` 值与驱动名一致
- 连接失败：检查 endpoint/host/port/鉴权配置
