// 本地可运行的临床数据接入与去标识化管线（持久化版）。
//
// 用法:
//
//	go run ./cmd/pipeline -dir <输入目录> [-state <状态目录>]
//	                     [-export <映射导出路径>] [-reidentify <去标识化ID>]
//
// 输入目录中 .txt 按结构化文本逐行处理，.json 按“资源对象边界”切分
// （单对象多行、顶层数组、多对象序列均可）；每个文件为一批，按文件名排序
// 提交。状态默认落在 ./.pipeline-state（0700 目录 / 0600 文件），
// 重跑与跨进程重启在同一状态目录上继续累积、结论与编号保持稳定。
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/pipeline"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/source"
	"github.com/highcumontoa/clinical-data-pipeline-go/internal/state"
)

func main() {
	dir := flag.String("dir", "", "输入目录（.txt 逐行、.json 按对象边界切分；每文件一批）")
	stateDir := flag.String("state", ".pipeline-state", "本地持久状态目录（0700）")
	exportPath := flag.String("export", "", "把重标识映射导出到该路径（0600，仅本地核对）")
	reid := flag.String("reidentify", "", "显式开启：按去标识化 ID 反查原始患者标识（仅本地排查）")
	flag.Parse()

	st, err := state.Open(*stateDir)
	if err != nil {
		if errors.Is(err, state.ErrLocked) {
			fmt.Fprintln(os.Stderr, "状态目录正被另一个管线进程占用：", *stateDir)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, "打开状态目录:", err)
		os.Exit(1)
	}
	defer st.Close()

	// 显式反查：默认不做，必须带 -reidentify 才执行，仅用于本地排查。
	if *reid != "" {
		canon, raw, ok := st.Registry().ReverseLookup(*reid)
		if !ok {
			fmt.Printf("deid=%s 未登记\n", *reid)
			os.Exit(3)
		}
		fmt.Printf("[REIDENTIFY 本地排查] deid=%s canonical=%s firstRawPatientId=%q\n", *reid, canon, raw)
	}

	// 只导出不摄入也是允许的用法。
	if *dir != "" {
		if err := runIngest(st, *dir); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	if *exportPath != "" {
		n, err := st.ExportMapping(*exportPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "导出映射:", err)
			os.Exit(1)
		}
		fmt.Printf("== mapping exported: %d entries -> %s (0600, 仅本地核对)\n", n, *exportPath)
	}

	latest := st.Latest()
	keys := make([]string, 0, len(latest))
	for k := range latest {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Println("== final state (latest per source/patient/type/code) ==")
	for _, k := range keys {
		ev := latest[k]
		fmt.Printf("  %s -> event=%s deid=%s at=%s value=%s\n",
			k, ev.EventID, ev.DeidentifiedID, ev.OccurredAt, ev.Value)
	}
	fmt.Printf("== dedup set size: %d (state: %s)\n", st.SeenCount(), *stateDir)
}

func runIngest(st *state.Store, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read dir: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		var msgs []pipeline.Message
		switch {
		case strings.HasSuffix(name, ".txt"):
			data, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				return fmt.Errorf("read file: %w", err)
			}
			// 文本消息按行；文件名（去扩展名）作为投递级来源系统。
			sysName := strings.TrimSuffix(name, ".txt")
			for _, line := range strings.Split(string(data), "\n") {
				if strings.TrimSpace(line) != "" {
					msgs = append(msgs, pipeline.Message{
						Source: model.SourceText, SourceSystem: sysName, Body: line})
				}
			}
		case strings.HasSuffix(name, ".json"):
			data, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				return fmt.Errorf("read file: %w", err)
			}
			sysName := strings.TrimSuffix(name, ".json")
			for _, ent := range source.SplitJSONStream(data) {
				if ent.Rejection != nil {
					// 词法级坏片段作为一条带预置拒绝原因的记录进入批次，整批回滚。
					msgs = append(msgs, pipeline.Message{
						SourceSystem:    sysName,
						PresetRejection: ent.Rejection,
					})
					continue
				}
				msgs = append(msgs, pipeline.Message{
					Source: model.SourceJSON, SourceSystem: sysName, Body: string(ent.Data)})
			}
		default:
			continue
		}

		res := pipeline.Durable(st, msgs)
		status := "COMMITTED"
		if res.RolledBack {
			status = "ROLLED_BACK"
		}
		fmt.Printf("== batch %s: %s (%d records)\n", name, status, len(msgs))
		for i, o := range res.Outcomes {
			if o.Quality.Accepted {
				fmt.Printf("  [%d] ACCEPT system=%s event=%s deid=%s code=%s at=%s value=%s\n",
					i, o.Event.SourceSystem, o.Event.EventID, o.Event.DeidentifiedID, o.Event.Code,
					o.Event.OccurredAt.Format("2006-01-02T15:04:05Z"), o.Event.Value)
			} else {
				r := o.Quality.Rejection
				fmt.Printf("  [%d] REJECT reason=%s field=%s detail=%s raw=%q\n",
					i, r.Reason, r.Field, r.Detail, r.RawHint)
			}
		}
		if res.Err != nil {
			return fmt.Errorf("批次 %s 落盘失败（未生效，可重放）: %w", name, res.Err)
		}
	}
	return nil
}
