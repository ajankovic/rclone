package accounting

import (
	"context"
	"sort"
	"sync"
)

type transferGroupCtx int64

const transferGroupKey transferGroupCtx = 1

// WithTransferGroup returns copy of the parent with assigned group name.
func WithTransferGroup(parent context.Context, group string) context.Context {
	return context.WithValue(parent, transferGroupKey, group)
}

// TransferGroupFromContext returns job ID from the context if it's available.
func TransferGroupFromContext(ctx context.Context) (string, bool) {
	group, ok := ctx.Value(transferGroupKey).(string)
	return group, ok
}

// FileSize represents file and it's size in bytes.
type FileSize struct {
	Name string `json:"name,omitempty"`
	Size int64  `json:"size,omitempty"`
}

// FileStatus contains information about the file scheduled for transfer.
type FileStatus struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	Sent int64  `json:"sent"`
}

type update struct {
	FileStatus
	Group string
}

// groups knows how to group file stats and update them when their transfer
// stats change.
type groups struct {
	done    chan struct{}
	updates chan update
	mu      sync.RWMutex
	files   map[string]map[string]FileStatus
}

func newGroups() *groups {
	g := &groups{
		done:    make(chan struct{}),
		updates: make(chan update, 2),
		files:   make(map[string]map[string]FileStatus),
	}

	go g.trackUpdates()

	return g
}

func (g *groups) trackUpdates() {
	for {
		select {
		case <-g.done:
			return
		case s := <-g.updates:
			g.mu.Lock()
			v := g.files[s.Group]
			if _, ok := v[s.Name]; !ok {
				g.mu.Unlock()
				continue
			}
			g.files[s.Group][s.Name] = s.FileStatus
			g.mu.Unlock()
		}
	}
}

func (g *groups) Close() error {
	close(g.done)
	return nil
}

func (g *groups) GroupStats(group string) []FileStatus {
	var l []FileStatus
	g.mu.RLock()
	for _, v := range g.files[group] {
		l = append(l, v)
	}
	g.mu.RUnlock()
	sort.Slice(l, func(i, j int) bool {
		if l[i].Name > l[j].Name {
			return false
		}
		return true
	})
	return l
}

func (g *groups) TrackGroup(group string, files []FileSize) {
	stats := make(map[string]FileStatus, len(files))
	for i := range files {
		if files[i].Size < 0 {
			continue
		}
		stats[files[i].Name] = FileStatus{
			Name: files[i].Name,
			Size: files[i].Size,
		}
	}
	g.mu.Lock()
	g.files[group] = stats
	g.mu.Unlock()
}

func (g *groups) Update(group string, s FileStatus) {
	g.updates <- update{s, group}
}

func (g *groups) RemoveGroup(group string) {
	g.mu.Lock()
	delete(g.files, group)
	g.mu.Unlock()
}
