package hlsvod

import (
	"fmt"
	"sync"

	"github.com/rs/zerolog"
)

// SegmentStatus represents the current state of a segment
type SegmentStatus int

const (
	// SegmentStatusNew means the segment hasn't been processed yet
	SegmentStatusNew SegmentStatus = iota
	// SegmentStatusQueued means the segment is queued for transcoding
	SegmentStatusQueued
	// SegmentStatusTranscoding means the segment is currently being transcoded
	SegmentStatusTranscoding
	// SegmentStatusCompleted means the segment has been successfully transcoded
	SegmentStatusCompleted
	// SegmentStatusError means there was an error transcoding the segment
	SegmentStatusError
)

// String returns the string representation of the SegmentStatus
func (s SegmentStatus) String() string {
	switch s {
	case SegmentStatusNew:
		return "new"
	case SegmentStatusQueued:
		return "queued"
	case SegmentStatusTranscoding:
		return "transcoding"
	case SegmentStatusCompleted:
		return "completed"
	case SegmentStatusError:
		return "error"
	default:
		return fmt.Sprintf("SegmentStatus(%d)", s)
	}
}

// DownloadStatus represents the current state of an HTTP segment request
//
// It is kept separate from SegmentStatus (which describes the *transcoding* pipeline)
// so that subscribers can observe both flows in a single combined update.
type DownloadStatus int

const (
	// DownloadStatusNone indicates the segment has never been requested yet
	DownloadStatusNone DownloadStatus = iota
	// DownloadStatusInFlight is set as soon as the HTTP handler parsed a segment request
	DownloadStatusInFlight
	// DownloadStatusSent is set right before the handler completes successfully
	DownloadStatusSent
	// DownloadStatusError marks a failed / aborted request
	DownloadStatusError
)

// String returns a string representation of the DownloadStatus.
func (d DownloadStatus) String() string {
	switch d {
	case DownloadStatusNone:
		return "none"
	case DownloadStatusInFlight:
		return "in_flight"
	case DownloadStatusSent:
		return "sent"
	case DownloadStatusError:
		return "error"
	default:
		return fmt.Sprintf("DownloadStatus(%d)", d)
	}
}

// StatusUpdate carries the latest *segment* and *download* status.
// For batch transcoding updates, DownloadStatus is nil.
// OldStatus is removed because consumers only need the fresh snapshot.
type StatusUpdate struct {
	SegmentIDs     []int           // list of segments affected; len==1 for single updates
	SegmentStatus  SegmentStatus   // meaningful for transcode-related events
	DownloadStatus *DownloadStatus // nil unless this update is about a single download
}

// Helper makeRange builds a slice [start, end).
func makeRange(start, end int) []int {
	if start >= end {
		return nil
	}
	r := make([]int, end-start)
	for i := range r {
		r[i] = start + i
	}
	return r
}

// Monitor tracks the state of segments during transcoding
type Monitor struct {
	segmentStatus    map[int]SegmentStatus
	downloadStatus   map[int]DownloadStatus
	segmentStatusMu  sync.RWMutex
	downloadStatusMu sync.RWMutex
	subscribers      []chan<- StatusUpdate
	subscribersMu    sync.RWMutex
	logger           zerolog.Logger
}

// NewMonitor creates a new Monitor instance
func NewMonitor(logger zerolog.Logger) *Monitor {
	return &Monitor{
		segmentStatus:  make(map[int]SegmentStatus),
		downloadStatus: make(map[int]DownloadStatus),
		subscribers:    make([]chan<- StatusUpdate, 0),
		logger:         logger,
	}
}

// Subscribe creates a new subscription for segment status updates
// The returned channel will receive updates when segment statuses change
// The channel has a buffer size of 10 to prevent blocking
func (m *Monitor) Subscribe() <-chan StatusUpdate {
	ch := make(chan StatusUpdate, 100)

	m.subscribersMu.Lock()
	m.subscribers = append(m.subscribers, ch)
	m.subscribersMu.Unlock()

	return ch
}

// notifySubscribers sends updates to all subscribers
func (m *Monitor) notifySubscribers(update StatusUpdate) {
	// avoid nil slices to prevent JSON null if marshalled
	if update.SegmentIDs == nil {
		update.SegmentIDs = []int{}
	}

	m.subscribersMu.RLock()
	defer m.subscribersMu.RUnlock()

	for _, sub := range m.subscribers {
		select {
		case sub <- update:
		default:
			ds := ""
			if update.DownloadStatus != nil {
				ds = update.DownloadStatus.String()
			}
			m.logger.Warn().
				Ints("segment_ids", update.SegmentIDs).
				Str("segment_status", update.SegmentStatus.String()).
				Str("download_status", ds).
				Msg("dropped status update due to full buffer")
		}
	}
}

// SetSegmentStatus updates the status of a single segment
func (m *Monitor) SetSegmentStatus(index int, status SegmentStatus) {
	m.segmentStatusMu.Lock()
	current := m.segmentStatus[index]
	if current == status {
		m.segmentStatusMu.Unlock()
		return
	}
	m.segmentStatus[index] = status
	m.segmentStatusMu.Unlock()

	// Notify subscribers in a goroutine to avoid blocking
	go m.notifySubscribers(StatusUpdate{
		SegmentIDs:     []int{index},
		SegmentStatus:  status,
		DownloadStatus: nil,
	})
}

// SetSegmentStatusRange updates the status of a range of segments [start, end)
// and emits a single notification for the entire range
func (m *Monitor) SetSegmentStatusRange(start, end int, status SegmentStatus) {
	if start >= end {
		return // Invalid range
	}

	m.segmentStatusMu.Lock()
	defer m.segmentStatusMu.Unlock()

	// Track if any segments were actually changed
	changed := false
	for i := start; i < end; i++ {
		if m.segmentStatus[i] != status {
			m.segmentStatus[i] = status
			changed = true
		}
	}

	// Send a single notification for the entire range if anything changed
	if changed {
		go func() {
			m.notifySubscribers(StatusUpdate{
				SegmentIDs:     makeRange(start, end),
				SegmentStatus:  status,
				DownloadStatus: nil,
			})
		}()
	}
}

// GetSegmentStatus retrieves the status of a segment
func (m *Monitor) GetSegmentStatus(index int) SegmentStatus {
	m.segmentStatusMu.RLock()
	defer m.segmentStatusMu.RUnlock()
	return m.segmentStatus[index]
}

// GetDownloadStatus retrieves the download status of a segment
func (m *Monitor) GetDownloadStatus(index int) DownloadStatus {
	m.downloadStatusMu.RLock()
	defer m.downloadStatusMu.RUnlock()
	return m.downloadStatus[index]
}

// SetDownloadStatus updates the download status and notifies subscribers
func (m *Monitor) SetDownloadStatus(index int, status DownloadStatus) {
	m.downloadStatusMu.Lock()
	m.downloadStatus[index] = status
	m.downloadStatusMu.Unlock()

	go m.notifySubscribers(StatusUpdate{
		SegmentIDs:     []int{index},
		SegmentStatus:  m.GetSegmentStatus(index),
		DownloadStatus: &status,
	})
}

// InitializeSegmentStatus initializes the status for all segments
func (m *Monitor) InitializeSegmentStatus(total int) {
	m.segmentStatusMu.Lock()
	defer m.segmentStatusMu.Unlock()
	m.segmentStatus = make(map[int]SegmentStatus, total)
	m.downloadStatus = make(map[int]DownloadStatus, total)
	for i := 0; i < total; i++ {
		m.segmentStatus[i] = SegmentStatusNew
		m.downloadStatus[i] = DownloadStatusNone
	}
}

// GetSegmentMap returns a string representation of segment states
func (m *Monitor) GetSegmentMap(totalSegments int) string {
	m.segmentStatusMu.RLock()
	defer m.segmentStatusMu.RUnlock()

	var result string
	for i := 0; i < totalSegments; i++ {
		status, exists := m.segmentStatus[i]
		if !exists {
			result += "–"
			continue
		}

		switch status {
		case SegmentStatusCompleted:
			result += "█"
		case SegmentStatusTranscoding:
			result += "▶"
		case SegmentStatusQueued:
			result += "□"
		case SegmentStatusError:
			result += "!"
		default:
			result += "·"
		}
	}
	return result
}

// GetSegmentCounts returns the count of segments in each state
func (m *Monitor) GetSegmentCounts(totalSegments int) (completed, inProgress, queued, errored, pending int) {
	m.segmentStatusMu.RLock()
	defer m.segmentStatusMu.RUnlock()

	for i := 0; i < totalSegments; i++ {
		switch m.segmentStatus[i] {
		case SegmentStatusCompleted:
			completed++
		case SegmentStatusTranscoding:
			inProgress++
		case SegmentStatusQueued:
			queued++
		case SegmentStatusError:
			errored++
		default:
			pending++
		}
	}
	return
}

// GetDownloadCounts returns the count of segments in each download state
func (m *Monitor) GetDownloadCounts(total int) (sent, requested, errored, none int) {
	m.downloadStatusMu.RLock()
	defer m.downloadStatusMu.RUnlock()
	for i := 0; i < total; i++ {
		switch m.downloadStatus[i] {
		case DownloadStatusSent:
			sent++
		case DownloadStatusInFlight:
			requested++
		case DownloadStatusError:
			errored++
		default:
			none++
		}
	}
	return
}

// GetDownloadMap returns a glyph bar representing download status for all segments.
// Glyphs:
//
//	✓  sent
//	▶  in flight
//	!  error
//	–  none/unrequested
func (m *Monitor) GetDownloadMap(total int) string {
	m.downloadStatusMu.RLock()
	defer m.downloadStatusMu.RUnlock()
	var result string
	for i := 0; i < total; i++ {
		switch m.downloadStatus[i] {
		case DownloadStatusSent:
			result += "✓"
		case DownloadStatusInFlight:
			result += "▶"
		case DownloadStatusError:
			result += "!"
		default:
			result += "–"
		}
	}
	return result
}

// LogSegmentMap logs the current segment map with the given note
func (m *Monitor) LogSegmentMap(note string, totalSegments int) {
	if !m.hasActivity() {
		return
	}

	completed, inProgress, queued, errored, _ := m.GetSegmentCounts(totalSegments)
	segmentMap := m.GetSegmentMap(totalSegments)

	logEvent := m.logger.Info()

	logEvent.
		Str("segments", segmentMap).
		Int("done", completed).
		Int("in_progress", inProgress).
		Int("queued", queued).
		Int("errored", errored).
		Msg(note)
}

// hasActivity checks if there's any transcoding activity to report
func (m *Monitor) hasActivity() bool {
	m.segmentStatusMu.RLock()
	defer m.segmentStatusMu.RUnlock()

	for _, status := range m.segmentStatus {
		if status != SegmentStatusNew {
			return true
		}
	}
	return false
}

// Close cleans up all resources used by the Monitor
// This should be called when the Monitor is no longer needed
func (m *Monitor) Close() {
	m.subscribersMu.Lock()
	defer m.subscribersMu.Unlock()

	// Close all subscriber channels
	for _, sub := range m.subscribers {
		close(sub)
	}
	m.subscribers = nil
}
