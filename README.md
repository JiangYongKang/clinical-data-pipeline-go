# 临床数据接入与去标识化管线

本地可长期运行的 Go 管线：把来自不同来源系统的患者事件归一化为统一模型，
把已提交结论与去标识化对应关系落到本地磁盘，保证同一患者在重复导入、并发导入、
跨进程重启、乱序投递与批次重放下都稳定落到同一个去标识化编号和同一条结论。

## 结构

- `internal/model` — 统一事件模型、拒绝原因（`RejectReason`）与数据质量判定（`Quality`）
- `internal/source` — 两类来源的解析与校验、JSON 对象流（单对象/多对象/数组）边界切分
- `internal/identity` — 身份归一化、暂存事务、冲突判定、去标识化映射、快照与反查
- `internal/pipeline` — 批次处理：按来源作用域去重、同号冲突拒绝、乱序取舍、提交/回滚
- `internal/state` — 本地持久化：原子快照、校验和、文件锁、权限收紧、映射导出
- `cmd/pipeline` — 本地运行入口（`run` / `export-mapping` / `reidentify`）

## 快速开始

```bash
go build ./...
go test ./... -race          # 全部测试（含竞态、崩溃重启端到端）
go build -o /tmp/pipeline ./cmd/pipeline

# 跑一个输入目录：每个文件为一批，按相对路径排序依次提交
/tmp/pipeline run ./examples/in --state ./.pipeline-state
```

输入目录递归遍历：`.txt` 按结构化文本处理，`.json` 按资源 JSON 处理。
**文件所在的一级子目录名就是来源系统**（如 `in/HIS-A/vitals.txt` 的来源系统为 `HIS-A`）；
直接放在输入目录根下的文件来源系统为 `local`。

## 来源与归一化规则

**结构化文本**（管道分隔，7 或 8 段，第 8 段为可选的来源系统）：

```
EVT|<eventID>|<patientID>|<eventType>|<code>|<RFC3339时间>|<值>[|<来源系统>]
```

**资源型 JSON**（声明结构，多余字段即拒绝；`sourceSystem` 可选）：

```json
{"resourceType":"Observation","id":"...","subject":"...","code":"...",
 "effectiveDateTime":"<RFC3339>","value":"...","sourceSystem":"HIS-A",
 "attrs":{"birthDate":"..."}}
```

一个 `.json` 文件的顶层可以是：**单个对象（允许任意缩进换行）**、
**多个对象并排**、或**一个对象数组**。管线按资源对象的边界读取，
再对每个对象独立做严格字段校验——因此排版放宽不会吞掉逐条错误。

归一化产出 `NormalizedEvent`：`SourceSystem / EventID / CanonicalPID /
DeidentifiedID / EventType / Code / OccurredAt(UTC) / Value`。

拒绝规则（不静默丢弃、不按默认值吞掉，原因可区分）：

| 原因 | 触发 |
|---|---|
| `MALFORMED` | 空消息、未知来源类型 |
| `STRUCTURE_MISMATCH` | 段数/类型头不符、JSON 顶层结构非法或含未声明字段、resourceType 非 Observation、时间非 RFC3339 |
| `MISSING_FIELD` | eventId / patientId / eventType / code / occurredAt / sourceSystem 缺失 |
| `INVALID_CODE` | 编码不在白名单（HR、BP-SYS、BP-DIA、TEMP、SPO2） |
| `IDENTITY_CONFLICT` | 同一规范患者键的身份属性冲突 |
| `EVENT_CONFLICT` | 同来源系统同一事件编号，但患者/时间/类型/编码/取值对不上且无法判定为同一件事 |

## 身份合并与冲突解决

- **归一化**：患者标识去首尾空白、忽略内部空白与 `-`/`_` 分隔符、统一大写。
  因此 `MRN-123`、` mrn 123 `、`mrn_123` 稳定归并为同一规范键 `MRN123`。
- **冲突判定**：对身份属性（`birthDate`、`familyName`），双方均提供且不一致时
  **明确拒绝**（`IDENTITY_CONFLICT`），绝不任选其一；一方未提供视为信息缺失，
  登记新值以收紧后续判定。
- **暂存事务**：解析阶段的身份解析只写入每批次的暂存视图；整批成功才合并进注册表，
  任一条被拒绝就丢弃暂存。因此被回滚的批次在身份登记里不留任何痕迹。
- **去标识化**：`DeidentifiedID = "PAT-" + SHA256("deid-v1:"+规范键)[:16]`（大写 hex）。
  纯函数推导、无随机性，跨进程、跨运行可复现；重复与并发导入同一患者得到同一 ID。

## 来源作用域去重与来源冲突

事件编号的作用域是**来源系统**，去重键为 `(sourceSystem, eventID)`：

- 不同来源系统里的相同编号是**两个不同事件**，不会互相顶掉
  （例：`HIS-A/e1` 与 `HIS-B/e1` 各自独立进入去重集合）。
- 同来源系统同编号、且患者/时间/类型/编码/取值**完全一致**：幂等重复投递，
  结论里标记 `DUP`，不新增身份、不新增去重键。
- 同来源系统同编号、但上述任一字段对不上：无法判定是否同一件事，
  **整批拒绝**（`EVENT_CONFLICT`），日志打印已存值与新到值，绝不静默挑一个。

乱序取舍仍与来源无关：状态键 `(规范患者键, 事件类型, 编码)` 下按
`(OccurredAt, EventID)` 字典序取最大者。

## 持久状态：存放位置与权限边界

- 默认状态目录 `./.pipeline-state`（`run --state <dir>` 可改），单文件
  `pipeline-state.json` 同时保存**已提交结论**与**规范键 → 去标识化编号**对应关系
  （含首次见到的一份原始患者标识，仅供显式反查）。
- **权限**：状态目录 `0700`、状态文件/锁文件/导出文件 `0600`，同机其他用户不可读。
  管线启动时会重新收紧已有目录/文件权限。
- **抗中断**：每次提交采用“临时文件(0600) → fsync → 原子 rename → fsync 目录”。
  rename 前崩溃只留下会在下次持锁启动时被清理的临时文件，正式文件永远是完整的；
  rename 后文件带 SHA-256 校验和，截断、损坏或被篡改时**拒绝载入**而非读到坏数据。
- **跨进程互斥**：状态目录上有 `flock` 排他锁，第二个管线进程会直接收到
  “状态目录已被另一个管线进程锁定”，避免并发写坏状态。
- **提交顺序**：先在内存完成预检与变更、产出合并快照，**先原子落盘成功，再承认提交**。
  落盘失败则本批内存变更与身份暂存一并回滚，磁盘与内存始终一致。

## 可复核导出与本地反查（适用范围）

这两项都涉及可还原信息，默认**不开启**，仅用于本地核对/排查：

```bash
# 导出一份对应关系（JSON，0600），必须用 -o 显式给出路径，不会随 run 自动产生
/tmp/pipeline export-mapping --state ./.pipeline-state -o ./mapping.audit.json

# 按去标识化编号反查规范键与首次留存的原始患者标识（命令会打印风险提示）
/tmp/pipeline reidentify --state ./.pipeline-state PAT-DD6101B96F0C9558
```

`run` 的常规输出只含去标识化编号与结论，不打印原始患者标识；
映射导出文件含原始标识，请以 0600 权限单独保管、用后删除。

## 批次提交/回滚、重启与重放

1. **批次原子性**：任一条记录被拒绝（解析/身份/同号冲突）则整批回滚，
   身份登记与结论状态都不变；把坏记录修好后重放同一批，结果与“一开始就全部成功”
   逐字段一致——去标识化编号、接收/拒绝判定、拒绝原因均相同
   （测试 `TestReplayAfterFailedBatchEqualsOneShot`）。
2. **重启可复现**：全新进程从状态目录载入后，结论与编号逐条一致；
   重复导入全部变成幂等 `DUP`（`TestCrossProcessRestartSameConclusions`）。
3. **提交途中被杀**：子进程在持久化点崩溃后重启重放，最终结论与一次性成功完全一致，
   再重跑全部幂等（端到端测试 `TestEndToEndCrashAndRestart`）。
4. **资源有界**：仅保存去重集合（不同 `(来源系统,EventID)` 数）与每个状态键的最新事件。

## 本地复现与验证

```bash
# 1) 准备多来源目录：HIS-A 文本、HIS-B 多行 JSON 与对象数组
mkdir -p /tmp/demo/in/HIS-A /tmp/demo/in/HIS-B
printf 'EVT|e1|MRN-123|observation|HR|2026-09-01T10:00:00Z|70\n' > /tmp/demo/in/HIS-A/a.txt
cat > /tmp/demo/in/HIS-B/b.json <<'JSON'
[
  {"resourceType":"Observation","id":"e1","subject":"mrn 123","code":"HR",
   "effectiveDateTime":"2026-09-02T08:00:00Z","value":"72",
   "attrs":{"birthDate":"1990-01-01"}}
]
JSON

# 2) 首次运行 + 立刻再跑一次（模拟重启）：同一编号、全部 DUP
/tmp/pipeline run /tmp/demo/in --state /tmp/demo/state
/tmp/pipeline run /tmp/demo/in --state /tmp/demo/state

# 3) 观察权限、导出、反查
ls -l /tmp/demo/state                 # 目录 0700、文件 0600
/tmp/pipeline export-mapping --state /tmp/demo/state -o /tmp/demo/map.json
/tmp/pipeline reidentify --state /tmp/demo/state $(grep -o 'PAT-[0-9A-F]*' /tmp/demo/map.json | head -1)
```

测试覆盖：畸形/缺字段/非法编码/结构不符（含多行、多对象、数组逐对象校验）、
跨来源同号不合并、同号内容冲突拒绝、身份冲突拒绝、重复/并发导入、乱序取舍、
批次失败重放与一次性成功一致、跨进程重启结论一致、提交途中崩溃恢复、
持久化损坏拒绝、权限 0700/0600、文件锁互斥、大规模输入资源有界。
