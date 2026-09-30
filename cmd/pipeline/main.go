// 本地可长期运行的临床数据接入与去标识化管线。
//
// 用法:
//
//	pipeline run <输入目录> [--state <状态目录>]
//	pipeline export-mapping --state <状态目录> -o <导出文件>
//	pipeline reidentify --state <状态目录> <去标识化编号...>
//
// run 递归遍历输入目录：.txt 按结构化文本处理，.json 按资源 JSON 处理，
// 每个文件为一批，按相对路径排序依次提交。文件所在的一级子目录名作为来源系统；
// 直接放在输入目录根下的文件来源系统为 "local"。
//
// 状态（已提交结论 + 去标识化对应关系）默认落在 ./.pipeline-state（0700/0600），
// 进程重启后自动载入，同一患者稳定落到同一编号与同一结论。
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/identity"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/pipeline"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/source"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/state"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "pipeline:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cmd := "run"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd = args[0]
		args = args[1:]
	}
	switch cmd {
	case "run":
		return cmdRun(args)
	case "export-mapping":
		return cmdExportMapping(args)
	case "reidentify":
		return cmdReidentify(args)
	case "-h", "--help", "help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("未知子命令 %q", cmd)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, strings.TrimSpace(`
usage:
  pipeline run <input-dir> [--state <state-dir>]
  pipeline export-mapping --state <state-dir> -o <output-file>
  pipeline reidentify --state <state-dir> <deidentified-id>...
`))
}

// parseFlags 是够用的最小标志解析：收集 --key value 形式参数与位置参数。
func parseFlags(args []string, bools map[string]bool) (positional []string, flags map[string]string, err error) {
	flags = map[string]string{}
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "-o":
			i++
			if i >= len(args) {
				return nil, nil, fmt.Errorf("-o 缺少参数")
			}
			flags["o"] = args[i]
		case strings.HasPrefix(a, "--"):
			key := strings.TrimPrefix(a, "--")
			if bools[key] {
				flags[key] = "true"
				continue
			}
			i++
			if i >= len(args) {
				return nil, nil, fmt.Errorf("--%s 缺少参数", key)
			}
			flags[key] = args[i]
		default:
			positional = append(positional, a)
		}
	}
	return positional, flags, nil
}

func stateDir(flags map[string]string) string {
	if d := flags["state"]; d != "" {
		return d
	}
	return state.DefaultDir
}

// crashPersister 仅用于端到端“提交途中被杀”测试：
// 设置 PIPELINE_CRASH=<去重事件总数阈值> 后，当持久化快照首次达到该事件数时
// 以非零状态退出，模拟进程在提交过程中被杀死。非测试运行不设置该变量即完全透传。
type crashPersister struct {
	inner pipeline.Persister
}

func (c *crashPersister) Persist(idSnap identity.Snapshot, storeSnap pipeline.StoreSnapshot) error {
	if v := os.Getenv("PIPELINE_CRASH"); v != "" {
		threshold := 0
		if _, err := fmt.Sscanf(v, "%d", &threshold); err == nil && threshold > 0 &&
			len(storeSnap.Seen) >= threshold {
			fmt.Fprintf(os.Stderr, "simulated crash after snapshot reached %d events\n",
				len(storeSnap.Seen))
			os.Exit(97)
		}
	}
	return c.inner.Persist(idSnap, storeSnap)
}

// inputFile 是一个待处理文件及其来源系统。
type inputFile struct {
	path   string
	name   string // 相对输入目录的路径（决定提交顺序）
	system string
}

func cmdRun(args []string) error {
	pos, flags, err := parseFlags(args, nil)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		usage()
		return fmt.Errorf("run 需要一个输入目录参数")
	}
	inputDir := pos[0]

	mgr := state.New(stateDir(flags))
	if err := mgr.Lock(); err != nil {
		return err
	}
	defer mgr.Unlock()

	idSnap, stSnap, err := mgr.Load()
	if err != nil {
		return fmt.Errorf("载入持久状态失败（拒绝以空状态覆盖）: %w", err)
	}
	reg := identity.NewRegistry()
	if err := reg.LoadSnapshot(idSnap); err != nil {
		return err
	}
	store := pipeline.NewStore()
	if err := store.LoadSnapshot(stSnap); err != nil {
		return err
	}
	p := pipeline.New(reg, pipeline.WithPersistence(&crashPersister{inner: mgr}))

	files, err := collectFiles(inputDir)
	if err != nil {
		return err
	}

	for _, f := range files {
		msgs, err := buildMessages(f)
		if err != nil {
			return fmt.Errorf("文件 %s 无法按资源边界读取: %w", f.name, err)
		}
		res := p.ProcessBatch(store, msgs)
		switch {
		case res.Err != nil:
			return fmt.Errorf("批次 %s 持久化失败，整批未生效: %w", f.name, res.Err)
		case res.RolledBack:
			fmt.Printf("== batch %s [system=%s]: ROLLED_BACK (%d records)\n", f.name, f.system, len(msgs))
		default:
			fmt.Printf("== batch %s [system=%s]: COMMITTED (%d records)\n", f.name, f.system, len(msgs))
		}
		for i, o := range res.Outcomes {
			if o.Quality.Accepted {
				dup := ""
				if o.Duplicate {
					dup = " DUP"
				}
				fmt.Printf("  [%d] ACCEPT system=%s event=%s deid=%s code=%s at=%s value=%s%s\n",
					i, o.Event.SourceSystem, o.Event.EventID, o.Event.DeidentifiedID, o.Event.Code,
					o.Event.OccurredAt.Format("2006-01-02T15:04:05Z"), o.Event.Value, dup)
			} else {
				rj := o.Quality.Rejection
				fmt.Printf("  [%d] REJECT reason=%s field=%s detail=%s raw=%q\n",
					i, rj.Reason, rj.Field, rj.Detail, rj.RawHint)
			}
		}
	}

	printFinalState(store)
	return nil
}

// collectFiles 递归收集输入文件并按相对路径排序；一级子目录名作为来源系统。
func collectFiles(inputDir string) ([]inputFile, error) {
	var files []inputFile
	err := filepath.WalkDir(inputDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		base := d.Name()
		if !strings.HasSuffix(base, ".txt") && !strings.HasSuffix(base, ".json") {
			return nil
		}
		rel, err := filepath.Rel(inputDir, path)
		if err != nil {
			return err
		}
		system := "local"
		if parts := strings.Split(filepath.ToSlash(rel), "/"); len(parts) > 1 {
			system = parts[0]
		}
		files = append(files, inputFile{path: path, name: filepath.ToSlash(rel), system: system})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("遍历输入目录失败: %w", err)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].name < files[j].name })
	return files, nil
}

// buildMessages 把一个文件转换为一批消息。
// .txt 按非空行切分；.json 按资源对象边界切分（单对象/多对象/数组均可）。
func buildMessages(f inputFile) ([]pipeline.Message, error) {
	data, err := os.ReadFile(f.path)
	if err != nil {
		return nil, err
	}
	var msgs []pipeline.Message
	if strings.HasSuffix(f.name, ".txt") {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			msgs = append(msgs, pipeline.Message{
				Source: model.SourceText, SourceSystem: f.system, Body: line,
			})
		}
		return msgs, nil
	}
	parts, err := source.SplitJSONObjectStream(data)
	if err != nil {
		return nil, err
	}
	for _, part := range parts {
		msgs = append(msgs, pipeline.Message{
			Source: model.SourceJSON, SourceSystem: f.system, Body: string(part),
		})
	}
	return msgs, nil
}

func printFinalState(store *pipeline.Store) {
	latest := store.Latest()
	keys := make([]string, 0, len(latest))
	for k := range latest {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Println("== final state (latest per patient/type/code) ==")
	for _, k := range keys {
		ev := latest[k]
		fmt.Printf("  %s -> system=%s event=%s deid=%s at=%s value=%s\n",
			k, ev.SourceSystem, ev.EventID, ev.DeidentifiedID,
			ev.OccurredAt.Format("2006-01-02T15:04:05Z"), ev.Value)
	}
	fmt.Printf("== dedup set size across sources: %d\n", store.SeenCount())
}

func cmdExportMapping(args []string) error {
	_, flags, err := parseFlags(args, nil)
	if err != nil {
		return err
	}
	out := flags["o"]
	if out == "" {
		return fmt.Errorf("export-mapping 必须用 -o 显式指定导出文件路径")
	}
	mgr := state.New(stateDir(flags))
	n, err := mgr.ExportMapping(out)
	if err != nil {
		return err
	}
	fmt.Printf("已导出 %d 条去标识化对应关系到 %s（权限 0600，仅用于本地核对）\n", n, out)
	return nil
}

func cmdReidentify(args []string) error {
	pos, flags, err := parseFlags(args, nil)
	if err != nil {
		return err
	}
	if len(pos) == 0 {
		return fmt.Errorf("reidentify 需要至少一个去标识化编号")
	}
	mgr := state.New(stateDir(flags))
	if err := mgr.Lock(); err != nil {
		return err
	}
	defer mgr.Unlock()
	idSnap, _, err := mgr.Load()
	if err != nil {
		return err
	}
	reg := identity.NewRegistry()
	if err := reg.LoadSnapshot(idSnap); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "注意：反查会暴露可还原的原始患者标识，仅限本地排查使用。")
	for _, deid := range pos {
		canon, raw, ok := reg.Reidentify(strings.TrimSpace(deid))
		if !ok {
			fmt.Printf("%s -> NOT FOUND\n", deid)
			continue
		}
		fmt.Printf("%s -> canonicalPid=%s rawPatientId=%q\n", deid, canon, raw)
	}
	return nil
}
