package state

import "testing"

func TestEventLessSubsecondBoundaries(t *testing.T) {
	ev := func(id, at string) Event {
		return testEvent("S", id, "MRN1", "HR", at, "v")
	}
	cases := []struct {
		name     string
		a, b     string // 时间串
		aID, bID string // 事件号（等时刻用例用同号，验证“不比时间大小”）
		want     bool   // eventLess(a,b)
		tag      string
	}{
		{"整秒小于半秒", "2026-09-01T10:00:00Z", "2026-09-01T10:00:00.5Z", "a", "b", true,
			"不带小数秒 vs 带小数秒（旧逻辑此处反了）"},
		{"半秒不小于整秒", "2026-09-01T10:00:00.5Z", "2026-09-01T10:00:00Z", "a", "b", false,
			"反向比较也必须正确"},
		{"等价值写法相等", "2026-09-01T10:00:00.5Z", "2026-09-01T10:00:00.50Z", "e1", "e1", false,
			".5 与 .50 同一时刻，不比时间大小"},
		{"固定宽度等价", "2026-09-01T10:00:00.500000000Z", "2026-09-01T10:00:00.5Z", "e1", "e1", false,
			"固定宽度串与可变宽度串同一时刻"},
		{"跨秒", "2026-09-01T10:00:00.9Z", "2026-09-01T10:00:01Z", "a", "b", true,
			"小数秒跨秒：00.9 < 01.0"},
		{"跨秒反向", "2026-09-01T10:00:01Z", "2026-09-01T10:00:00.9Z", "a", "b", false, ""},
		{"不同位数比大小", "2026-09-01T10:00:00.249Z", "2026-09-01T10:00:00.25Z", "a", "b", true,
			".249 < .25（字典序会判反）"},
		{"不同位数比大小反向", "2026-09-01T10:00:00.25Z", "2026-09-01T10:00:00.249Z", "a", "b", false, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.tag != "" {
				t.Logf("业务覆盖：%s", c.tag)
			}
			got := eventLess(ev(c.aID, c.a), ev(c.bID, c.b))
			if got != c.want {
				t.Fatalf("eventLess(%s,%s)=%v want %v", c.a, c.b, got, c.want)
			}
			t.Logf("比较结果 %s < %s => %v", c.a, c.b, got)
		})
	}

	// 时间完全相同：按事件号分先后，稳定确定性。
	lo := ev("e1", "2026-09-01T10:00:00Z")
	hi := ev("e2", "2026-09-01T10:00:00Z")
	if !eventLess(lo, hi) || eventLess(hi, lo) {
		t.Fatal("时间相同时必须按事件号严格分先后")
	}
	t.Log("时间完全相同：事件号 e1 < e2 稳定取舍")
}

func TestPayloadEqualSubsecondWording(t *testing.T) {
	a := testEvent("S", "e1", "MRN1", "HR", "2026-09-01T10:00:00.5Z", "70")
	b := testEvent("S", "e1", "MRN1", "HR", "2026-09-01T10:00:00.500000000Z", "70")
	if !PayloadEqual(a, b) {
		t.Fatal(".5 与 .500000000 为同一时刻，重放不得误报 EVENT_CONFLICT")
	}
	t.Log("同一时刻不同小数秒写法 => 载荷一致（重放幂等，不误报冲突）")

	c := testEvent("S", "e1", "MRN1", "HR", "2026-09-01T10:00:00.500000001Z", "70")
	if PayloadEqual(a, c) {
		t.Fatal("真实时刻不同必须判为载荷不一致")
	}
	t.Log("真实时刻不同（.5 vs .500000001）=> 载荷不一致，冲突判定不放松")
}
