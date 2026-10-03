package core

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"
)

// Go stack capture into the backend's frame shape.
//
// This is the deliberate divergence from the Node SDK. In JavaScript a thrown
// Error carries a string `.stack` that the SDK parses with regexes. In Go an
// `error` value carries no stack, so the SDK must capture one itself:
//
//   - CurrentStack walks the live call stack with runtime.Callers — used for
//     CaptureException/CaptureMessage at the call site.
//   - ParseGoStack parses the text of runtime/debug.Stack() — used on the panic
//     recovery path, where the panicking frames have already unwound and are
//     only recoverable from the goroutine's printed stack.
//
// Frames in the Go runtime, the standard library, or the module cache are
// marked InApp=false, exactly as the Node SDK flags node_modules/runtime
// frames, so fingerprinting groups on application code only.

// sdkPathPrefix is this module's import path. Frames inside the SDK are skipped
// from the top of a captured stack so the first reported frame is the caller's.
const sdkPathPrefix = "github.com/OpenResearchGuys/reelay-go-sdk"

var goRoot = runtime.GOROOT()

func isInApp(file, fn string) bool {
	if strings.HasPrefix(fn, "runtime.") || strings.HasPrefix(fn, "runtime/") {
		return false
	}
	if strings.Contains(file, "/pkg/mod/") {
		return false
	}
	if goRoot != "" && strings.HasPrefix(file, goRoot) {
		return false
	}
	return true
}

// CurrentStack captures the live call stack as frames, skipping frames that
// belong to the SDK itself so the first frame is the application caller.
func CurrentStack(limit int) []StackFrame {
	if limit <= 0 {
		limit = 50
	}
	pcs := make([]uintptr, 64)
	// Skip runtime.Callers and CurrentStack itself.
	n := runtime.Callers(2, pcs)
	if n == 0 {
		return nil
	}
	frames := runtime.CallersFrames(pcs[:n])
	out := make([]StackFrame, 0, limit)
	skippingSDK := true
	for {
		f, more := frames.Next()
		inSDK := strings.HasPrefix(f.Function, sdkPathPrefix)
		if skippingSDK && inSDK {
			if !more {
				break
			}
			continue
		}
		skippingSDK = false
		out = append(out, StackFrame{
			File:     f.File,
			Line:     f.Line,
			Function: shortFunc(f.Function),
			InApp:    isInApp(f.File, f.Function),
		})
		if len(out) >= limit || !more {
			break
		}
	}
	return out
}

// ParseGoStack parses the text produced by runtime/debug.Stack() into frames.
// The format alternates a function line and a tab-indented "\tfile:line +0x.."
// location line:
//
//	goroutine 1 [running]:
//	main.charge(0x1)
//		/app/checkout/service.go:142 +0x1d
//	main.main()
//		/app/main.go:8 +0x2a
func ParseGoStack(stack []byte, limit int) []StackFrame {
	if limit <= 0 {
		limit = 50
	}
	lines := strings.Split(string(stack), "\n")
	out := make([]StackFrame, 0, limit)
	for i := 0; i+1 < len(lines) && len(out) < limit; i++ {
		fnLine := strings.TrimSpace(lines[i])
		if fnLine == "" || strings.HasPrefix(fnLine, "goroutine ") {
			continue
		}
		locLine := strings.TrimSpace(lines[i+1])
		if !strings.Contains(locLine, ".go:") {
			continue
		}
		// locLine looks like "/app/main.go:8 +0x2a"; take the first field.
		loc := strings.Fields(locLine)[0]
		file, lineNo, ok := splitFileLine(loc)
		if !ok {
			continue
		}
		fn := funcFromLine(fnLine)
		out = append(out, StackFrame{
			File:     file,
			Line:     lineNo,
			Function: shortFunc(fn),
			InApp:    isInApp(file, fn),
		})
		i++ // consume the location line
	}
	return out
}

// splitFileLine splits "/path/file.go:142" into ("/path/file.go", 142).
func splitFileLine(s string) (file string, line int, ok bool) {
	idx := strings.LastIndex(s, ":")
	if idx < 0 {
		return "", 0, false
	}
	n, err := strconv.Atoi(s[idx+1:])
	if err != nil {
		return "", 0, false
	}
	return s[:idx], n, true
}

// funcFromLine extracts the fully-qualified function name from a stack function
// line, stripping the argument list ("main.charge(0x1)" -> "main.charge") and a
// leading "created by " on goroutine-origin lines.
func funcFromLine(line string) string {
	line = strings.TrimPrefix(line, "created by ")
	if idx := strings.IndexByte(line, '('); idx >= 0 {
		line = line[:idx]
	}
	return strings.TrimSpace(line)
}

// shortFunc trims the package path from a fully-qualified function name,
// keeping the trailing package.Func form for readability.
func shortFunc(fn string) string {
	if fn == "" {
		return ""
	}
	if slash := strings.LastIndex(fn, "/"); slash >= 0 {
		return fn[slash+1:]
	}
	return fn
}

// NormalizeError extracts a {type, value} pair from any recovered/thrown value,
// defensively, mirroring the Node SDK's normalizeError. Stack frames are
// captured separately on Go because errors do not carry them.
func NormalizeError(v any) (typ string, value string) {
	switch e := v.(type) {
	case nil:
		return "Error", "<nil>"
	case error:
		return errorType(e), e.Error()
	case string:
		return "Error", e
	case fmt.Stringer:
		return "Error", e.String()
	default:
		if b, err := json.Marshal(v); err == nil {
			s := string(b)
			if len(s) > 1000 {
				s = s[:1000]
			}
			return "NonErrorThrown", s
		}
		return "NonErrorThrown", fmt.Sprintf("%v", v)
	}
}

// errorType derives a human-readable type name from an error's dynamic type,
// e.g. "*fmt.wrapError" -> "wrapError". The generic *errors.errorString is
// reported as "Error" since its concrete name carries no signal.
func errorType(err error) string {
	t := reflect.TypeOf(err)
	for t != nil && t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t == nil {
		return "Error"
	}
	name := t.Name()
	if name == "" || name == "errorString" {
		return "Error"
	}
	return name
}
