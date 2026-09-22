# clinical-data-pipeline-go

本地可运行的临床数据接入与去标识化管线：把两个不同来源的患者事件
解析、归一化为统一模型，完成跨来源患者身份归并、确定性去标识化，
并保证重复投递、乱序到达、批次中途失败与重放下结论稳定可复现。

无第三方依赖，仅使用 Go 标准库（Go ≥ 1.25）。

## 目录结构

```
cmd/pipeline/          本地 CLI：扫描 inbox 目录、分批处理、打印结论
internal/model/        统一模型、质量判定与可区分的拒绝原因
internal/normalize/    两类来源的解析与归一化
internal/identity/     患者身份归并、性别冲突检测
internal/deid/         HMAC 确定性假名与重标识映射边界
internal/store/        文件锁 + WAL journal + 原子提交/回滚
internal/pipeline/     批次编排：并发解析、去重、乱序规则、提交
scripts/local-demo.sh  一键本地验证
```

## 支持的输入

收件箱按“来源系统”分子目录，文件扩展名决定格式：

```
inbox/<sourceSystem>/*.txt    结构化文本消息
inbox/<sourceSystem>/*.json   资源型 JSON
```

文件名带 `BATCH-<id>__` 前缀的归入同一批次（用于失败/重放语义），
其余文件归入一个 ad-hoc 批次。

### 1. 结构化文本消息（.txt）

竖线分隔的 `key=value`：

```
MID=E001|PID= p-001 |TYPE=ADMISSION|TIME=2026-01-01T10:00:00Z|GENDER=M
```

- 必填：`MID`（记录号）、`PID`（患者号）、`TYPE`、`TIME`（RFC3339）
- 可选：`GENDER`，取值 `M/F/O/U`
- `TYPE`：`ADMISSION | DISCHARGE | OBSERVATION | DIAGNOSIS`（大小写不敏感）
- 未知字段、重复字段、非 `key=value` 段均为结构错误
- 时间统一归一化为 UTC

### 2. 资源型 JSON（.json）

固定形状的 `ClinicalEvent` 资源：

```json
{
  "resourceType": "ClinicalEvent",
  "recordId": "J1",
  "patientId": "P_777",
  "eventType": "DIAGNOSIS",
  "eventTime": "2026-02-03T08:30:00+02:00",
  "gender": "F"
}
```

未知 JSON 字段、值类型错误、根对象后尾随数据一律拒绝，而不是忽略。

### 归一化规则（确定性）

| 项 | 规则 |
|---|---|
| 编码 | 事件类型/性别大写后映射到规范枚举；时间转 RFC3339 UTC |
| 空白 | 字段值去除首尾空白；患者号额外去除内部空白（见下） |
| 内容指纹 | `SHA-256(system|recordId|eventType|eventTime(UTC)|gender)`；不受 PID 表示差异影响，用于识别“同记录不同内容”的冲突重投 |

## 拒绝原因（可区分，绝不静默丢弃/按默认值吞掉）

| 原因 | 触发情形 |
|---|---|
| `MISSING_REQUIRED_FIELD` | 必填字段缺失或空白；患者号归一化后为空；无来源系统 |
| `INVALID_CODE` | 事件类型/性别不在规范取值内 |
| `MALFORMED_MESSAGE` | 文本非 key=value、JSON 语法错误、根对象后有尾随数据、未知来源类型 |
| `STRUCTURE_MISMATCH` | 文本含未知/重复字段、时间不是 RFC3339；JSON 未知字段、值类型错误、resourceType 不是 ClinicalEvent |
| `IDENTITY_CONFLICT` | 同一身份的已知性别（M/F/O）出现互相矛盾的断言 |
| `DUPLICATE_CONTENT_CONFLICT` | 同一 (system, recordId) 重投但内容指纹不同 |
| `LATE_OUT_OF_ORDER` | 事件时间早于 `水位 - LateGrace` 宽限窗口 |

每条拒绝都带字段级 `Detail`（判定依据）、载荷哈希与 `REJECTED` 质量判定。

## 身份合并与冲突解决规则

1. **规范化键**：患者号经 Unicode 大写、剔除所有空白字符、剔除
   分隔符 `- _ : /` 得到规范键。因此 `p-001`、` P 001 `、`P:001`
   稳定合并为同一身份；规范化后为空则拒绝。
2. **跨来源**：不同拼写不做模糊匹配；只有显式别名表
   （`-aliases`，构建时校验空值与环路）可以把一个旧号指向既定身份。
3. **性别合并**：`未知(U)/缺省` 从不冲突，且让位于已知性别；
   两个不同的已知性别冲突时**明确拒绝**（`IDENTITY_CONFLICT`），
   不会任选其一。
4. **并发**：解析阶段有界并发；批次与身份解析全程互斥，重复导入、
   并发导入同一患者只产生一个身份键与一个假名，不会错误合并。

## 去标识化与重标识的边界

- 假名：`PSN-<hex(HMAC-SHA256(secret, 身份键)[:16])>`，确定性算法，
  同一 `(secret, 身份键)` 永远得到同一 token；不同键不可碰撞。
- **去标识侧**：事件流只出现假名，不含原始患者号；无 secret 时
  HMAC 单向不可逆。
- **重标识侧**：`state/token-map.json` 是 pseudonym→身份键映射，
  以 `0600` 权限随批次事务落盘，应单独访问控制、与脱敏数据隔离保管。
- 未知 token 的重标识请求返回 false，绝不猜测身份。
- 重放/新进程会先加载既有映射，因此“先到先得”的映射稳定不漂移。

## 批次、提交/回滚、乱序与去重

- **WAL**：处理前写 `journal/<batchId>.json`；仅在原子提交后删除。
- **提交**：结果文件（事件/拒绝）与 `state.json`（去重表、身份性别、
  水位、已提交批次指纹）全部走 temp-file+rename 原子替换；进程持有
  `flock(LOCK_EX)` 独占锁。
- **回滚**：中途任何内部错误（含注入故障）删除 journal，已提交文件
  不受影响；重开库时残留 journal 视为已回滚并清理。**部分失败后
  重放的结果与一次性成功逐字节一致**（有测试断言）。
- **批次幂等**：同一 `batchId` 重放是空操作。
- **重复投递**：相同 (system, recordId)+相同内容指纹 → 幂等忽略
  （计入 `Duplicates`，非拒绝、不产生第二条事件）；同号不同内容 →
  `DUPLICATE_CONTENT_CONFLICT`。
- **乱序**：批次内按 `(eventTime, sourceSystem, recordId)` 确定性
  排序后应用，平局与到达顺序无关；水位 = 已应用事件的最大
  eventTime，早于 `水位 - LateGrace` 的事件确定性拒绝
  （`LATE_OUT_OF_ORDER`），窗口内的迟到事件照常接受。
- **资源有界**：解析 worker 数固定（`-workers`，默认 4），与输入
  规模无关；5000 条/2 worker 的场景有测试覆盖。

## 本地验证

```bash
# 一键演示（构造两类来源、重复、乱序、畸形、身份冲突并断言）
./scripts/local-demo.sh

# 或手动：
go test ./...                 # 全量单测
go test -race ./...           # 竞态检测
go vet ./...

printf 'local-dev-pepper' > secret.key
go run ./cmd/pipeline -inbox ./examples/inbox -state ./state \
    -secret ./secret.key -grace 48h -workers 4
# 再次运行（新进程重放）→ accepted=0 rejected=0 duplicates=0，
# state/events 与 state/token-map.json 与首次运行逐字节一致
```

测试覆盖：畸形/缺字段/非法编码/结构不符输入、重复与乱序投递、
窗口内外迟到事件、注入式中途失败+重放等于一次性成功、同 batchId
幂等、并发导入同一患者不产生重复身份/假名、5000 条有界并发批，
日志中打印关键标识（recordId、身份键、假名、拒绝原因、Detail）。
