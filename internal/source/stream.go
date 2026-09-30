// 按资源对象边界从一个 JSON 输入里切分出多条记录。
//
// 支持三种文件形态，互不影响：
//   - 单条资源对象（可以排版成多行、任意缩进）
//   - 顶层数组 [ {...}, {...} ]（元素也可跨行）
//   - 一个文件里顺序放置多个顶层对象（JSON 序列流，允许逗号分隔）
//
// 切分按词法边界进行（字符串内括号与空白不会误判）；
// 未闭合片段（如 {oops）只产生一条 MALFORMED 记录，不影响同文件其余对象摄入。
// 每个切出的片段仍交给 ParseJSONWithSystem 做严格结构校验，
// 多余字段、缺字段、非法时间/编码各自产生可区分的拒绝原因。
package source

import (
	"encoding/json"
	"fmt"

	"github.com/highcumontoa/clinical-data-pipeline-go/internal/model"
)

// StreamEntry 是切分后的一个输入片段。
type StreamEntry struct {
	Data      []byte           // 原始 JSON 片段（可直接交给 ParseJSON*）
	Rejection *model.Rejection // 片段本身词法不合法时非空
}

// SplitJSONStream 按资源对象边界切分一个 JSON 文件。
func SplitJSONStream(data []byte) []StreamEntry {
	var out []StreamEntry
	i := 0
	for {
		i = skipWS(data, i)
		if i >= len(data) {
			return out
		}
		switch data[i] {
		case '{', '[':
			open, close := data[i], byte('}')
			if data[i] == '[' {
				close = ']'
			}
			end, ok := matchBracket(data, i, open, close)
			if !ok {
				out = append(out, StreamEntry{Rejection: reject(
					model.RejectMalformed, "",
					fmt.Sprintf("%s 未闭合，JSON 值被截断", string(open)),
					trimmedRemainder(data, i))})
				return out
			}
			raw := append([]byte(nil), data[i:end+1]...)
			if open == '[' {
				var els []json.RawMessage
				if err := json.Unmarshal(raw, &els); err != nil {
					out = append(out, StreamEntry{Rejection: reject(
						model.RejectMalformed, "",
						fmt.Sprintf("数组词法非法: %v", err), string(raw))})
					return out
				}
				for _, el := range els {
					out = append(out, StreamEntry{Data: append([]byte(nil), el...)})
				}
			} else {
				out = append(out, StreamEntry{Data: raw})
			}
			i = end + 1
		default:
			// 顶层标量（字符串/数字/字面量等）：原样交出，由结构校验给出拒绝原因；
			// 完全不成词法的垃圾片段按 MALFORMED 处理后停止。
			end, ok := scanScalar(data, i)
			if !ok {
				out = append(out, StreamEntry{Rejection: reject(
					model.RejectMalformed, "", "无法识别的 JSON 词法片段",
					trimmedRemainder(data, i))})
				return out
			}
			out = append(out, StreamEntry{Data: append([]byte(nil), data[i:end]...)})
			i = end
		}
		// 顶层值之间允许空白或单个逗号。
		j := skipWS(data, i)
		if j < len(data) && data[j] == ',' {
			i = j + 1
		} else {
			i = j
		}
	}
}

// matchBracket 从 open 位置做括号配平，返回闭合括号下标。
func matchBracket(data []byte, start int, open, close byte) (int, bool) {
	depth := 0
	inStr := false
	esc := false
	for i := start; i < len(data); i++ {
		c := data[i]
		if inStr {
			switch {
			case esc:
				esc = false
			case c == '\\':
				esc = true
			case c == '"':
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
		case open:
			depth++
		case close:
			depth--
			if depth == 0 {
				return i, true
			}
		}
	}
	return -1, false
}

// scanScalar 扫描顶层标量值，返回值结束的下标。
func scanScalar(data []byte, start int) (int, bool) {
	i := start
	switch {
	case data[i] == '"':
		esc := false
		i++
		for ; i < len(data); i++ {
			switch {
			case esc:
				esc = false
			case data[i] == '\\':
				esc = true
			case data[i] == '"':
				return i + 1, true
			}
		}
		return -1, false // 字符串未闭合
	case data[i] == '-' || (data[i] >= '0' && data[i] <= '9'):
		for i < len(data) && !isDelim(data[i]) {
			i++
		}
		return i, true
	case isLiteralStart(data[i]):
		for i < len(data) && !isDelim(data[i]) {
			i++
		}
		return i, true
	default:
		return -1, false
	}
}

func isLiteralStart(c byte) bool {
	return c == 't' || c == 'f' || c == 'n'
}

func isDelim(c byte) bool {
	switch c {
	case ' ', '\t', '\r', '\n', ',', ']', '}', '{', '[':
		return true
	}
	return false
}

func skipWS(data []byte, i int) int {
	for i < len(data) {
		switch data[i] {
		case ' ', '\t', '\r', '\n':
			i++
		default:
			return i
		}
	}
	return i
}

func trimmedRemainder(data []byte, i int) string {
	rem := data[i:]
	if len(rem) > maxRawHint {
		return string(rem[:maxRawHint]) + "..."
	}
	return string(rem)
}
