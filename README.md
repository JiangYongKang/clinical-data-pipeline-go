# 临床数据接入与去标识化管线

本地可运行的 Go 管线：把来自不同来源的患者事件归一化为统一模型，
保证同一患者在多来源、重复与乱序投递下的结论稳定可复现。

## 结构

- `internal/model` — 统一事件模型、拒绝原因（`RejectReason`）与数据质量判定（`Quality`）
- `internal/source` — 两类来源的解析与校验
- `internal/identity` — 患者身份归一化、归并、冲突判定、去标识化映射
- `internal/pipeline` — 批次处理：去重、乱序取舍、提交/回滚
- `cmd/pipeline` — 本地运行入口

## 来源与归一化规则

**结构化文本**（管道分隔，7 段）：

```
EVT|<eventID>|<patientID>|<eventType>|<code>|<RFC3339时间>|<值>
```

**资源型 JSON**（声明结构，多余字段即拒绝）：

```json
{"resourceType":"Observation","id":"...","subject":"...","code":"...",
 "effectiveDateTime":"<RFC3339>","value":"...","attrs":{"birthDate":"..."}}
```

归一化产出 `NormalizedEvent`：`EventID / CanonicalPID / DeidentifiedID /
EventType / Code / OccurredAt(UTC) / Value`。

拒绝规则（不静默丢弃、不按默认值吞掉，原因可区分）：

| 原因 | 触发 |
|---|---|
| `MALFORMED` | 空消息、未知来源类型 |
| `STRUCTURE_MISMATCH` | 段数/类型头不符、JSON 含未声明字段、resourceType 非 Observation、时间非 RFC3339 |
| `MISSING_FIELD` | eventId / patientId / eventType / code / occurredAt 缺失 |
| `INVALID_CODE` | 编码不在白名单（HR、BP-SYS、BP-DIA、TEMP、SPO2） |
| `IDENTITY_CONFLICT` | 同一规范患者键的身份属性冲突 |

## 身份合并与冲突解决

- **归一化**：患者标识去首尾空白、忽略内部空白与 `-`/`_` 分隔符、统一大写。
  因此 `MRN-123`、` mrn 123 `、`mrn_123` 稳定归并为同一规范键 `MRN123`。
- **冲突判定**：对身份属性（`birthDate`、`familyName`），双方均提供且不一致时
  **明确拒绝**（`IDENTITY_CONFLICT`），绝不任选其一；一方未提供视为信息缺失，
  登记新值以收紧后续判定。
- **去标识化**：`DeidentifiedID = "PAT-" + SHA256("deid-v1:"+规范键)[:16]`（大写 hex）。
  纯函数推导，无随机性，跨进程、跨运行可复现；重复与并发导入同一患者
  得到同一 ID（注册表加锁，幂等）。
- **重标识边界**：重标识映射即 `规范键 -> DeidentifiedID`，仅存在于进程内
  `identity.Registry`，不随输出落盘；管线对外输出只含 `DeidentifiedID`
  与规范键，不含原始患者标识。需要重标识时，在同一进程内用规范键查询注册表。

## 批次提交/回滚与确定性取舍

1. **批次原子性**：任一条记录被拒绝则整批回滚，已提交状态不变；
   修复后重放与一次性成功结果完全一致（测试 `TestBatchRollbackAndReplay` 断言）。
2. **去重**：按 `EventID` 去重，首次投递生效，重复投递为幂等空操作。
3. **乱序取舍**：状态键为 `(规范患者键, 事件类型, 编码)`，
   按 `(OccurredAt, EventID)` 字典序取最大者，与到达顺序无关。
4. **资源有界**：仅保存去重集合（不同 EventID 数）与每个状态键的最新事件，
   无无界缓冲；2 万条同键事件处理后状态键仍为 1（`TestLargeInputBounded`）。

## 本地验证

```bash
go build ./...                 # 编译
go test ./... -v -race         # 全部测试（含竞态检测），日志打印关键标识与判定依据

# 端到端运行：目录内 .txt 按文本消息、.json 按资源 JSON 处理，每个文件为一批
go run ./cmd/pipeline <输入目录>
```

测试覆盖：畸形与缺字段输入、非法编码、结构不符、重复投递、乱序到达、
批次中途失败重放、身份冲突拒绝、并发导入同一患者、大规模输入资源有界。
