package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestEndToEndCrashAndRestart 构建真实二进制，模拟“提交途中进程被杀”，
// 然后重启重放，断言结论与从未崩溃的一次性运行完全一致。
func TestEndToEndCrashAndRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("subprocess end-to-end test skipped in -short")
	}
	bin := filepath.Join(t.TempDir(), "pipeline")
	build := exec.Command("go", "build", "-o", bin, ".")
	var buildErr bytes.Buffer
	build.Stderr = &buildErr
	if err := build.Run(); err != nil {
		t.Fatalf("build binary: %v\n%s", err, buildErr.String())
	}

	writeInputs := func(t *testing.T, dir string) {
		t.Helper()
		mustMkdir(t, filepath.Join(dir, "HIS-A"))
		mustMkdir(t, filepath.Join(dir, "HIS-B"))
		mustWrite(t, filepath.Join(dir, "HIS-A", "a.txt"), strings.Join([]string{
			"EVT|e1|MRN-123|observation|HR|2026-09-01T10:00:00Z|70",
			"EVT|e2|MRN-123|observation|HR|2026-09-01T12:00:00Z|82",
		}, "\n")+"\n")
		mustWrite(t, filepath.Join(dir, "HIS-B", "b.json"),
			`[{"resourceType":"Observation","id":"e3","subject":"mrn 456","code":"TEMP",`+
				`"effectiveDateTime":"2026-09-02T08:00:00Z","value":"36.5"}]`+"\n")
	}

	// 路径 A：处理第二批（HIS-B）时在持久化点“被杀”，随后重启重放。
	rootA := t.TempDir()
	inA := filepath.Join(rootA, "in")
	stateA := filepath.Join(rootA, "state")
	writeInputs(t, inA)

	// 阈值 3：第二份文件使 seen 达到 3 时，Persist 在落盘前崩溃退出。
	crash := exec.Command(bin, "run", inA, "--state", stateA)
	crash.Env = append(os.Environ(), "PIPELINE_CRASH=3")
	crashOut, _ := crash.CombinedOutput()
	if crash.ProcessState == nil || crash.ProcessState.ExitCode() != 97 {
		t.Fatalf("expected crash exit 97, got state=%v output=%s",
			crash.ProcessState, crashOut)
	}
	t.Logf("crashed mid-commit as designed:\n%s", lastLines(crashOut, 2))

	// 重启（同一状态目录，不注入崩溃）重放全部输入。
	replay := exec.Command(bin, "run", inA, "--state", stateA)
	replayOut, err := replay.CombinedOutput()
	if err != nil {
		t.Fatalf("replay after crash failed: %v\n%s", err, replayOut)
	}
	gotFinal := finalStateBlock(replayOut)
	if len(gotFinal) < 2 {
		t.Fatalf("replay final state too small:\n%s", replayOut)
	}
	t.Logf("replay final state:\n%s", strings.Join(gotFinal, "\n"))

	// 路径 B：全新目录一次性成功。
	rootB := t.TempDir()
	inB := filepath.Join(rootB, "in")
	stateB := filepath.Join(rootB, "state")
	writeInputs(t, inB)
	oneshot := exec.Command(bin, "run", inB, "--state", stateB)
	oneshotOut, err := oneshot.CombinedOutput()
	if err != nil {
		t.Fatalf("one-shot run failed: %v\n%s", err, oneshotOut)
	}
	wantFinal := finalStateBlock(oneshotOut)

	if strings.Join(gotFinal, "\n") != strings.Join(wantFinal, "\n") {
		t.Fatalf("crash-replay conclusions differ from one-shot:\n replay:\n%s\n one-shot:\n%s",
			strings.Join(gotFinal, "\n"), strings.Join(wantFinal, "\n"))
	}

	// 再重跑一次：全部应为幂等重复，去重集不变。
	again := exec.Command(bin, "run", inA, "--state", stateA)
	againOut, err := again.CombinedOutput()
	if err != nil {
		t.Fatalf("second replay failed: %v", err)
	}
	if !strings.Contains(string(againOut), "dedup set size across sources: 3") {
		t.Fatalf("dedup size must remain 3 after replays:\n%s", againOut)
	}
	if strings.Count(string(againOut), "DUP") < 3 {
		t.Fatalf("all redelivered events should be DUP:\n%s", againOut)
	}
	t.Log("crash-replay identical to one-shot; subsequent run fully idempotent")
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
}

// finalStateBlock 截取输出里 "== final state" 到结尾的结论行（去掉提示头）。
func finalStateBlock(out []byte) []string {
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var block []string
	inBlock := false
	for _, ln := range lines {
		if strings.HasPrefix(ln, "== final state") {
			inBlock = true
			continue
		}
		if strings.HasPrefix(ln, "== dedup") {
			continue
		}
		if inBlock {
			block = append(block, strings.TrimSpace(ln))
		}
	}
	return block
}

func lastLines(b []byte, n int) string {
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) <= n {
		return string(b)
	}
	return strings.Join(lines[len(lines)-n:], "\n")
}
