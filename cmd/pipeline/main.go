// 本地可运行的临床数据接入与去标识化管线。
// 用法: go run ./cmd/pipeline <输入目录>
// 输入目录中 .txt 按结构化文本处理，.json 按资源型 JSON 处理；
// 每个文件为一批，按文件名排序依次提交，输出确定。
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
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: pipeline <input-dir>")
		os.Exit(2)
	}
	dir := os.Args[1]
	entries, err := os.ReadDir(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "read dir:", err)
		os.Exit(1)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	reg := identity.NewRegistry()
	p := pipeline.New(reg)
	store := pipeline.NewStore()

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var src model.SourceType
		switch {
		case strings.HasSuffix(e.Name(), ".txt"):
			src = model.SourceText
		case strings.HasSuffix(e.Name(), ".json"):
			src = model.SourceJSON
		default:
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			fmt.Fprintln(os.Stderr, "read file:", err)
			os.Exit(1)
		}
		var msgs []pipeline.Message
		for _, line := range strings.Split(string(data), "\n") {
			if strings.TrimSpace(line) != "" {
				msgs = append(msgs, pipeline.Message{Source: src, Body: line})
			}
		}
		res := p.ProcessBatch(store, msgs)
		status := "COMMITTED"
		if res.RolledBack {
			status = "ROLLED_BACK"
		}
		fmt.Printf("== batch %s: %s (%d records)\n", e.Name(), status, len(msgs))
		for i, o := range res.Outcomes {
			if o.Quality.Accepted {
				fmt.Printf("  [%d] ACCEPT event=%s deid=%s code=%s at=%s value=%s\n",
					i, o.Event.EventID, o.Event.DeidentifiedID, o.Event.Code,
					o.Event.OccurredAt.Format("2006-01-02T15:04:05Z"), o.Event.Value)
			} else {
				r := o.Quality.Rejection
				fmt.Printf("  [%d] REJECT reason=%s field=%s detail=%s raw=%q\n",
					i, r.Reason, r.Field, r.Detail, r.RawHint)
			}
		}
	}

	latest := store.Latest()
	keys := make([]string, 0, len(latest))
	for k := range latest {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Println("== final state (latest per patient/type/code) ==")
	for _, k := range keys {
		ev := latest[k]
		fmt.Printf("  %s -> event=%s deid=%s at=%s value=%s\n",
			k, ev.EventID, ev.DeidentifiedID,
			ev.OccurredAt.Format("2006-01-02T15:04:05Z"), ev.Value)
	}
	fmt.Printf("== dedup set size: %d\n", store.SeenCount())
}
