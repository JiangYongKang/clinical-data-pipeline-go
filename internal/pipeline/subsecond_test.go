// 小数秒乱序取舍：同一患者/类型/编码下，仅小数秒不同的事件，
// 无论正序、乱序、分批还是跨进程重启重放，最终都必须留下真实时间更晚的那条。
package pipeline

import (
	"testing"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/state"
)

// subCase 描述一组“只差在小数秒”的两条事件及期望结论。
type subCase struct {
	name     string
	earlyID  string // 事件号（时间更早的一条）
	earlyTS  string
	lateID   string
	lateTS   string
	equal    bool   // 时间完全相同：靠事件号分先后，lateID 为事件号更大者
	coverTag string // 覆盖的业务边界
}

func subsecondCases() []subCase {
	return []subCase{
		{"整秒与一位小数", "eA", "2026-09-02T10:00:00Z", "eB", "2026-09-02T10:00:00.5Z", false,
			"不带小数秒 vs 带小数秒（用户实际场景）"},
		{"一位与两位小数", "eA", "2026-09-02T10:00:00.2Z", "eB", "2026-09-02T10:00:00.25Z", false,
			"小数秒位数不同 .2 vs .25"},
		{"等价值不同写法", "eA", "2026-09-02T10:00:00.5Z", "eB", "2026-09-02T10:00:00.50Z", true,
			".5 与 .50 为同一时刻，靠事件号分先后"},
		{"两位与三位小数", "eA", "2026-09-02T10:00:00.249Z", "eB", "2026-09-02T10:00:00.25Z", false,
			".249 vs .25 真实时间先后"},
		{"小数秒跨秒", "eA", "2026-09-02T10:00:00.9Z", "eB", "2026-09-02T10:00:01Z", false,
			"跨秒：...:00.9 vs 下一秒整秒"},
		{"纳秒精度", "eA", "2026-09-02T10:00:00.123456788Z", "eB", "2026-09-02T10:00:00.123456789Z", false,
			"9 位小数秒相邻值"},
		{"完全相同时间", "eA", "2026-09-02T10:00:00Z", "eB", "2026-09-02T10:00:00Z", true,
			"时间完全相同，事件号大者胜（确定性取舍）"},
	}
}

// winnerID 取出唯一结论键上的胜出事件号。
func winnerID(t *testing.T, st *state.Store) string {
	t.Helper()
	latest := st.Latest()
	if len(latest) != 1 {
		t.Fatalf("期望恰好 1 个结论键，实际 %d", len(latest))
	}
	for _, ev := range latest {
		return ev.EventID
	}
	return ""
}

func TestDurableSubsecondSelection(t *testing.T) {
	for _, c := range subsecondCases() {
		t.Run(c.name, func(t *testing.T) {
			t.Logf("业务覆盖：%s", c.coverTag)
			wantWinner := c.lateID // 非同时刻取更晚；同时刻事件号更大者胜
			early := dText("HIS-A", c.earlyID, "MRN-2", "HR", c.earlyTS, "v-"+c.earlyID)
			late := dText("HIS-A", c.lateID, "MRN-2", "HR", c.lateTS, "v-"+c.lateID)

			// 场景 1：同一批正序到达（早的在前）。
			dirFwd := t.TempDir()
			stFwd, _ := state.Open(dirFwd)
			if r := Durable(stFwd, []Message{early, late}); !r.Committed {
				t.Fatalf("正序批未提交: %+v", r)
			}
			if w := winnerID(t, stFwd); w != wantWinner {
				t.Fatalf("正序到达应留 %s，实际留 %s", wantWinner, w)
			}
			t.Logf("正序到达：%s(%s) -> %s(%s)，结论=%s（正确）",
				c.earlyID, c.earlyTS, c.lateID, c.lateTS, wantWinner)
			stFwd.Close()

			// 场景 2：同一批乱序到达（晚的在前）。
			dirRev := t.TempDir()
			stRev, _ := state.Open(dirRev)
			if r := Durable(stRev, []Message{late, early}); !r.Committed {
				t.Fatalf("乱序批未提交: %+v", r)
			}
			if w := winnerID(t, stRev); w != wantWinner {
				t.Fatalf("乱序到达应留 %s，实际留 %s", wantWinner, w)
			}
			t.Logf("乱序到达：结论与正序一致=%s", wantWinner)
			stRev.Close()

			// 场景 3：分两批到达（晚的一批先到，早的一批后到）。
			dirSplit := t.TempDir()
			stSplit, _ := state.Open(dirSplit)
			if r := Durable(stSplit, []Message{late}); !r.Committed {
				t.Fatalf("首批未提交: %+v", r)
			}
			if r := Durable(stSplit, []Message{early}); !r.Committed {
				t.Fatalf("次批未提交: %+v", r)
			}
			if w := winnerID(t, stSplit); w != wantWinner {
				t.Fatalf("分批乱序到达应留 %s，实际留 %s", wantWinner, w)
			}
			if stSplit.SeenCount() != 2 {
				t.Fatalf("分批后 seen 应为 2，实际 %d", stSplit.SeenCount())
			}
			t.Logf("分批到达（晚批先到）：结论=%s，seen=2", wantWinner)
			stSplit.Close()

			// 场景 4：跨进程重启后读盘，结论不变。
			stReopen, err := state.Open(dirFwd)
			if err != nil {
				t.Fatalf("重启打开失败: %v", err)
			}
			defer stReopen.Close()
			if w := winnerID(t, stReopen); w != wantWinner {
				t.Fatalf("跨进程重启后结论漂移：应留 %s，实际留 %s", wantWinner, w)
			}
			if stReopen.SeenCount() != 2 {
				t.Fatalf("重启后 seen 应为 2，实际 %d", stReopen.SeenCount())
			}
			t.Logf("跨进程重启：结论=%s 稳定，seen=2", wantWinner)

			// 场景 5：重启后把同一批原样再导一遍——幂等，不新增身份/事件，结论不变。
			beforeSeen := stReopen.SeenCount()
			if r := Durable(stReopen, []Message{early, late}); !r.Committed {
				t.Fatalf("重放批未提交: %+v", r)
			}
			if stReopen.SeenCount() != beforeSeen {
				t.Fatalf("重放后 seen 漂移：%d -> %d", beforeSeen, stReopen.SeenCount())
			}
			if n := countPatients(stReopen); n != 1 {
				t.Fatalf("重放后患者身份数应为 1，实际 %d", n)
			}
			if w := winnerID(t, stReopen); w != wantWinner {
				t.Fatalf("重放后结论漂移：应留 %s，实际留 %s", wantWinner, w)
			}
			t.Logf("重启后重放同一批：幂等，seen=%d、身份=1、结论=%s 均不变",
				stReopen.SeenCount(), wantWinner)
		})
	}
}

// TestSubsecondReplayEqualsOneShot：含小数秒的批次，“先建状态再重放”
// 与“一次性导入”的最终落盘结论必须逐项一致。
func TestSubsecondReplayEqualsOneShot(t *testing.T) {
	msgs := []Message{
		dText("HIS-A", "e1", "MRN-3", "HR", "2026-09-03T08:00:00Z", "70"),
		dText("HIS-A", "e2", "MRN-3", "HR", "2026-09-03T08:00:00.5Z", "71"),
		dText("HIS-A", "e3", "MRN-3", "HR", "2026-09-03T07:59:59.9Z", "69"),
	}

	dirA := t.TempDir()
	stA, _ := state.Open(dirA)
	if r := Durable(stA, msgs[:2]); !r.Committed {
		t.Fatalf("首批未提交: %+v", r)
	}
	stA.Close()
	// 新进程重放整批（含已提交的两条 + 新的一条）。
	stA2, _ := state.Open(dirA)
	if r := Durable(stA2, msgs); !r.Committed {
		t.Fatalf("重放批未提交: %+v", r)
	}
	defer stA2.Close()

	dirB := t.TempDir()
	stB, _ := state.Open(dirB)
	defer stB.Close()
	if r := Durable(stB, msgs); !r.Committed {
		t.Fatalf("一次性批未提交: %+v", r)
	}

	la, lb := stA2.Latest(), stB.Latest()
	if len(la) != len(lb) {
		t.Fatalf("结论键数不一致：%d vs %d", len(la), len(lb))
	}
	for k, ev := range lb {
		if la[k] != ev {
			t.Fatalf("重放与一次性导入结论不一致 key=%s: %+v vs %+v", k, la[k], ev)
		}
	}
	if stA2.SeenCount() != stB.SeenCount() {
		t.Fatalf("seen 不一致：%d vs %d", stA2.SeenCount(), stB.SeenCount())
	}
	if n := countPatients(stA2); n != 1 || countPatients(stB) != 1 {
		t.Fatalf("身份数异常：replay=%d oneshot=%d", countPatients(stA2), countPatients(stB))
	}
	t.Logf("小数秒场景重放==一次性导入：键=%d seen=%d 胜出=%s（真实时间最晚的 e2）",
		len(la), stB.SeenCount(), lb["MRN3|observation|HR"].EventID)
}
