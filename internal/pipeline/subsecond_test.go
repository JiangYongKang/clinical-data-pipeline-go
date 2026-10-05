package pipeline

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/state"
)

// subsecondCase 描述“同一患者/类型/编码、仅时间小数部分不同”的一对事件。
type subsecondCase struct {
	desc   string // 覆盖的业务边界
	older  string // 真实时间更早（或同时刻但事件号更小）的时间戳
	newer  string // 真实时间更晚（或同时刻但事件号更大）的时间戳
	olderE string // 更早事件的事件号
	newerE string // 更晚事件的事件号
}

func subsecondCases() []subsecondCase {
	return []subsecondCase{
		{"整秒 vs 同秒半秒（带/不带小数秒写法等价）",
			"2026-09-01T10:00:00Z", "2026-09-01T10:00:00.5Z", "e-a", "e-b"},
		{"小数位前缀：.1 与 .15（字符串序会反向的隐蔽场景）",
			"2026-09-01T10:00:00.1Z", "2026-09-01T10:00:00.15Z", "e-a", "e-b"},
		{"不同位数：.25 与 .5",
			"2026-09-01T10:00:00.25Z", "2026-09-01T10:00:00.5Z", "e-a", "e-b"},
		{"跨秒：x.9 与下一秒整秒",
			"2026-09-01T10:00:00.9Z", "2026-09-01T10:00:01Z", "e-a", "e-b"},
		{"时刻完全相同（.5 与 .50）：按事件号分先后",
			"2026-09-01T10:00:00.50Z", "2026-09-01T10:00:00.5Z", "e-a", "e-b"},
	}
}

func fracMsg(id, ts string) Message {
	return dText("HIS", id, "MRN-1", "HR", ts, "v-"+id)
}

// expectWinner 断言状态键上留下的是 newer 事件，且规模仍为 seen=2、身份=1、键=1。
func expectWinner(t *testing.T, st *state.Store, c subsecondCase) {
	t.Helper()
	latest := st.Latest()
	if len(latest) != 1 {
		t.Fatalf("[%s] 状态键数=%d，期望 1", c.desc, len(latest))
	}
	if got := st.SeenCount(); got != 2 {
		t.Fatalf("[%s] seen=%d，期望 2（不得多出/丢失事件）", c.desc, got)
	}
	if n := countPatients(st); n != 1 {
		t.Fatalf("[%s] 患者身份数=%d，期望恰好 1（不得多登记）", c.desc, n)
	}
	for _, ev := range latest {
		if ev.EventID != c.newerE {
			t.Fatalf("[%s] 留下事件 %s，期望真实时间更晚的 %s（at=%s）",
				c.desc, ev.EventID, c.newerE, ev.OccurredAt)
		}
		t.Logf("  结果：留下 %s at=%s value=%s（正确，按真实时间精度取舍）",
			ev.EventID, ev.OccurredAt, ev.Value)
	}
}

// TestSubsecondOrdering 覆盖同一键下仅差小数秒的事件取舍：
// 正序一批、乱序一批、分两批（含跨进程重启）、整批导入后跨进程重启再重放，
// 最终结论都必须是真实时间更晚的事件，且重放幂等。
func TestSubsecondOrdering(t *testing.T) {
	for _, c := range subsecondCases() {
		t.Run(c.desc, func(t *testing.T) {
			older := fracMsg(c.olderE, c.older)
			newer := fracMsg(c.newerE, c.newer)

			t.Logf("业务：%s；更早=%s(%s) 更晚=%s(%s)",
				c.desc, c.olderE, c.older, c.newerE, c.newer)

			// 1) 正序同一批到达。
			t.Log("  用例 1：两条事件正序一批到达")
			dir := t.TempDir()
			st, err := state.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			if r := Durable(st, []Message{older, newer}); !r.Committed {
				t.Fatalf("正序批次未提交: %+v", r)
			}
			expectWinner(t, st, c)
			st.Close()

			// 2) 乱序（更晚的先到）同一批到达。
			t.Log("  用例 2：两条事件乱序一批到达（更晚先到）")
			dir = t.TempDir()
			st, _ = state.Open(dir)
			if r := Durable(st, []Message{newer, older}); !r.Committed {
				t.Fatalf("乱序批次未提交: %+v", r)
			}
			expectWinner(t, st, c)
			st.Close()

			// 3) 分两批到达，批次之间模拟跨进程重启。
			t.Log("  用例 3：更早事件先进一批，重启进程后更晚事件再进一批")
			dir = t.TempDir()
			st, _ = state.Open(dir)
			if r := Durable(st, []Message{older}); !r.Committed {
				t.Fatalf("分批第一批未提交: %+v", r)
			}
			st.Close()
			st, _ = state.Open(dir)
			if r := Durable(st, []Message{newer}); !r.Committed {
				t.Fatalf("分批第二批未提交: %+v", r)
			}
			expectWinner(t, st, c)
			st.Close()

			// 4) 整批导入后，跨进程重启并重放同一批：结论不变、规模不变（幂等）。
			t.Log("  用例 4：整批导入 -> 跨进程重启 -> 同数据再导一遍（重放幂等）")
			dir = t.TempDir()
			st, _ = state.Open(dir)
			if r := Durable(st, []Message{older, newer}); !r.Committed {
				t.Fatalf("首轮未提交: %+v", r)
			}
			st.Close()
			st, _ = state.Open(dir)
			if r := Durable(st, []Message{older, newer}); !r.Committed {
				t.Fatalf("重放未提交: %+v", r)
			}
			expectWinner(t, st, c)
			st.Close()

			// 再开一次确认落盘结论在跨进程重启后依旧正确。
			st, _ = state.Open(dir)
			expectWinner(t, st, c)
			st.Close()
		})
	}
}

// TestSubsecondHealsPersistedWrongLatest 复现“旧版本把错结论落盘”的现场：
// 手工把 state.json 的 latest 改成更早的事件（旧字符串序 bug 的产物），
// 重新打开（模拟跨进程重启）后，结论必须自愈为更晚事件并回写磁盘；
// 随后重放同一批不得多登记身份、不得多出事件。
func TestSubsecondHealsPersistedWrongLatest(t *testing.T) {
	dir := t.TempDir()

	// 先用当前版本正常导入两条事件（身份、seen、映射都是真实一致的）。
	st, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	older := fracMsg("e-a", "2026-09-01T10:00:00Z")
	newer := fracMsg("e-b", "2026-09-01T10:00:00.5Z")
	if r := Durable(st, []Message{older, newer}); !r.Committed {
		t.Fatalf("首轮未提交: %+v", r)
	}
	st.Close()

	// 篡改磁盘结论：把 latest 指回更早的 e-a，模拟旧版本落盘的错误状态。
	path := filepath.Join(dir, "state.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var disk map[string]json.RawMessage
	if err := json.Unmarshal(raw, &disk); err != nil {
		t.Fatal(err)
	}
	var seen map[string]state.Event
	if err := json.Unmarshal(disk["seen"], &seen); err != nil {
		t.Fatal(err)
	}
	wrongLatest := map[string]state.Event{
		state.LatestKey("MRN1", "observation", "HR"): seen[state.DedupKey("HIS", "e-a")],
	}
	lraw, _ := json.Marshal(wrongLatest)
	disk["latest"] = lraw
	fixed, _ := json.MarshalIndent(disk, "", "  ")
	if err := os.WriteFile(path, fixed, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Log("业务：磁盘上的旧错误结论 latest=e-a(整秒)，seen 同时含 e-a 与 e-b(.5秒)")

	// 跨进程重启：打开即应自愈。
	st2, err := state.Open(dir)
	if err != nil {
		t.Fatalf("重开失败: %v", err)
	}
	c := subsecondCase{desc: "旧错误落盘状态自愈", olderE: "e-a", newerE: "e-b"}
	expectWinner(t, st2, c)

	// 磁盘文件本身也必须已被修正。
	ondisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var healed struct {
		Latest map[string]state.Event `json:"latest"`
	}
	if err := json.Unmarshal(ondisk, &healed); err != nil {
		t.Fatal(err)
	}
	if ev := healed.Latest[state.LatestKey("MRN1", "observation", "HR")]; ev.EventID != "e-b" {
		t.Fatalf("磁盘结论未修正，落盘仍是 %s", ev.EventID)
	}
	t.Log("  结果：重启后内存与磁盘结论均自愈为 e-b(at=...00.5Z)，seen=2、身份=1")

	// 重放同一批：幂等，无新增身份/事件，结论不漂移。
	if r := Durable(st2, []Message{older, newer}); !r.Committed {
		t.Fatalf("重放未提交: %+v", r)
	}
	expectWinner(t, st2, c)
	st2.Close()

	// 再重启确认稳定。
	st3, _ := state.Open(dir)
	defer st3.Close()
	expectWinner(t, st3, c)
}
