package reelay

import (
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OpenResearchGuys/reelay-go-sdk/core"
)

// Continuous wall-clock profiling.
//
// Go's runtime CPU profiler emits protobuf that would force a heavy
// dependency on both ends, so the SDK uses the classic dependency-free
// alternative (the approach Pyroscope's early Go integration took): sample
// every goroutine's stack with runtime.Stack on a fixed period, keep only
// goroutines that are actually running/runnable/in a syscall, and fold the
// samples into stack counts. That approximates an on-CPU profile and also
// surfaces syscall-bound work; the chunk is typed "wall" to be honest about
// the methodology.
//
// Duty cycle (the Pyroscope/Parca continuous-profiling pattern): profile
// `window` out of every `cycle`, so steady-state overhead stays in the low
// single digits. runtime.Stack stops the world briefly per sample; the
// default 100ms period over a 10s window is 100 samples per minute.

const (
	defaultProfileWindow = 10 * time.Second
	defaultProfileCycle  = 60 * time.Second
	profileSamplePeriod  = 100 * time.Millisecond
	maxStackBufBytes     = 1 << 20 // 1 MB of goroutine dump per sample
)

type continuousProfiler struct {
	window time.Duration
	cycle  time.Duration
	send   func(core.ProfileChunkPayload)
	debug  func(string, any)

	mu      sync.Mutex
	stopped chan struct{}
}

func newContinuousProfiler(window, cycle time.Duration, send func(core.ProfileChunkPayload), debug func(string, any)) *continuousProfiler {
	if window <= 0 {
		window = defaultProfileWindow
	}
	if cycle < window {
		cycle = defaultProfileCycle
	}
	if cycle < window {
		cycle = window
	}
	return &continuousProfiler{window: window, cycle: cycle, send: send, debug: debug}
}

func (p *continuousProfiler) start() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped != nil {
		return
	}
	p.stopped = make(chan struct{})
	go p.loop(p.stopped)
}

func (p *continuousProfiler) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped != nil {
		close(p.stopped)
		p.stopped = nil
	}
}

func (p *continuousProfiler) loop(stopped chan struct{}) {
	defer func() {
		if r := recover(); r != nil && p.debug != nil {
			p.debug("profiler crashed", r)
		}
	}()
	ticker := time.NewTicker(p.cycle)
	defer ticker.Stop()

	// First window immediately, then one per cycle.
	p.runWindow(stopped)
	for {
		select {
		case <-stopped:
			return
		case <-ticker.C:
			p.runWindow(stopped)
		}
	}
}

func (p *continuousProfiler) runWindow(stopped chan struct{}) {
	startedAt := time.Now().UTC()
	counts := map[string]*core.ProfileStackPayload{}
	var sampleCount int64

	sampleTicker := time.NewTicker(profileSamplePeriod)
	defer sampleTicker.Stop()
	deadline := time.After(p.window)

	buf := make([]byte, maxStackBufBytes)
	for {
		select {
		case <-stopped:
			return
		case <-deadline:
			endedAt := time.Now().UTC()
			chunk := foldSamples(counts, sampleCount, startedAt, endedAt)
			if chunk != nil {
				p.send(*chunk)
			}
			return
		case <-sampleTicker.C:
			n := runtime.Stack(buf, true)
			for _, stack := range parseGoroutineDump(string(buf[:n])) {
				key := stackKey(stack)
				if existing, ok := counts[key]; ok {
					existing.Count++
				} else {
					counts[key] = &core.ProfileStackPayload{Frames: stack, Count: 1}
				}
				sampleCount++
			}
		}
	}
}

func foldSamples(counts map[string]*core.ProfileStackPayload, sampleCount int64, startedAt, endedAt time.Time) *core.ProfileChunkPayload {
	if len(counts) == 0 {
		return nil
	}
	stacks := make([]core.ProfileStackPayload, 0, len(counts))
	for _, s := range counts {
		stacks = append(stacks, *s)
	}
	return &core.ProfileChunkPayload{
		ChunkID:        "prof_" + core.NewEventID()[4:],
		ProfileType:    "wall",
		Platform:       "go",
		SamplePeriodMS: float64(profileSamplePeriod) / float64(time.Millisecond),
		SampleCount:    sampleCount,
		StartedAt:      startedAt.Format(time.RFC3339Nano),
		EndedAt:        endedAt.Format(time.RFC3339Nano),
		Stacks:         stacks,
	}
}

func stackKey(frames []core.ProfileFramePayload) string {
	var b strings.Builder
	for _, f := range frames {
		b.WriteString(f.Function)
		b.WriteByte('|')
		b.WriteString(f.File)
		b.WriteByte(':')
		b.WriteString(strconv.Itoa(f.Line))
		b.WriteByte(';')
	}
	return b.String()
}

// activeStates are the goroutine states worth sampling: on CPU, ready to
// run, or executing a syscall. Parked goroutines (chan receive, IO wait,
// select, sleep…) are skipped so the profile shows work, not waiting.
func isActiveState(state string) bool {
	switch {
	case strings.HasPrefix(state, "running"),
		strings.HasPrefix(state, "runnable"),
		strings.HasPrefix(state, "syscall"):
		return true
	}
	return false
}

// parseGoroutineDump parses runtime.Stack(all=true) output into root-first
// frame slices, one per active goroutine, dropping the profiler's own stack
// and pure-runtime goroutines.
func parseGoroutineDump(dump string) [][]core.ProfileFramePayload {
	var out [][]core.ProfileFramePayload
	for _, block := range strings.Split(dump, "\n\n") {
		lines := strings.Split(block, "\n")
		if len(lines) < 2 {
			continue
		}
		header := lines[0]
		if !strings.HasPrefix(header, "goroutine ") {
			continue
		}
		open := strings.IndexByte(header, '[')
		closing := strings.LastIndexByte(header, ']')
		if open < 0 || closing <= open {
			continue
		}
		if !isActiveState(header[open+1 : closing]) {
			continue
		}

		// Function/location line pairs, leaf-first.
		var frames []core.ProfileFramePayload
		for i := 1; i+1 < len(lines); i += 2 {
			fn := strings.TrimSpace(lines[i])
			loc := strings.TrimSpace(lines[i+1])
			if fn == "" || loc == "" {
				break
			}
			// "main.work(0x1, 0x2)" → "main.work"; "created by …" ends the list.
			if strings.HasPrefix(fn, "created by ") {
				break
			}
			if p := strings.LastIndexByte(fn, '('); p > 0 {
				fn = fn[:p]
			}
			file, line := parseLocation(loc)
			frames = append(frames, core.ProfileFramePayload{
				Function: fn,
				File:     file,
				Line:     line,
				InApp:    isInAppFrame(fn, file),
			})
		}
		if len(frames) == 0 {
			continue
		}
		// Skip the profiler's own sampling goroutine.
		if strings.Contains(frames[0].Function, "reelay-go-sdk.(*continuousProfiler)") ||
			strings.Contains(frames[0].Function, "runtime.Stack") {
			continue
		}
		reverseFrames(frames) // leaf-first → root-first
		out = append(out, frames)
	}
	return out
}

// parseLocation splits "\t/app/main.go:42 +0x1a" into file and line.
func parseLocation(loc string) (string, int) {
	if i := strings.IndexByte(loc, ' '); i >= 0 {
		loc = loc[:i]
	}
	colon := strings.LastIndexByte(loc, ':')
	if colon < 0 {
		return loc, 0
	}
	line, err := strconv.Atoi(loc[colon+1:])
	if err != nil {
		return loc, 0
	}
	return loc[:colon], line
}

func isInAppFrame(fn, file string) bool {
	if strings.HasPrefix(fn, "runtime.") || strings.HasPrefix(fn, "reflect.") {
		return false
	}
	if strings.Contains(file, "/go/pkg/mod/") || strings.Contains(file, "\\go\\pkg\\mod\\") {
		return false
	}
	if strings.Contains(file, runtime.GOROOT()) && runtime.GOROOT() != "" {
		return false
	}
	return true
}

func reverseFrames(frames []core.ProfileFramePayload) {
	for i, j := 0, len(frames)-1; i < j; i, j = i+1, j-1 {
		frames[i], frames[j] = frames[j], frames[i]
	}
}
