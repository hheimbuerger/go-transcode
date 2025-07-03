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

// SegmentStatusUpdate represents a change in segment status
type SegmentStatusUpdate struct {
	SegmentID int
	OldStatus SegmentStatus
	NewStatus SegmentStatus
}

// Monitor tracks the state of segments during transcoding
type Monitor struct {
	segmentStatus   map[int]SegmentStatus
	segmentStatusMu sync.RWMutex
	subscribers     []chan<- SegmentStatusUpdate
	subscribersMu   sync.RWMutex
	logger          zerolog.Logger
}

// NewMonitor creates a new Monitor instance
func NewMonitor(logger zerolog.Logger) *Monitor {
	return &Monitor{
		segmentStatus: make(map[int]SegmentStatus),
		subscribers:   make([]chan<- SegmentStatusUpdate, 0),
		logger:        logger,
	}
}

// Subscribe creates a new subscription for segment status updates
// The returned channel will receive updates when segment statuses change
// The channel has a buffer size of 10 to prevent blocking
func (m *Monitor) Subscribe() <-chan SegmentStatusUpdate {
	ch := make(chan SegmentStatusUpdate, 10) // Buffer size of 10 as requested

	m.subscribersMu.Lock()
	m.subscribers = append(m.subscribers, ch)
	m.subscribersMu.Unlock()

	return ch
}

// notifySubscribers sends updates to all subscribers
func (m *Monitor) notifySubscribers(update SegmentStatusUpdate) {
	m.subscribersMu.RLock()
	defer m.subscribersMu.RUnlock()

	for _, sub := range m.subscribers {
		select {
		case sub <- update:
			// Message sent successfully
		default:
			// Skip if subscriber's buffer is full
			m.logger.Warn().
				Int("segment_id", update.SegmentID).
				Str("old_status", update.OldStatus.String()).
				Str("new_status", update.NewStatus.String()).
				Msg("Dropped segment status update due to full buffer")
		}
	}
}

// SetSegmentStatus updates the status of a single segment
func (m *Monitor) SetSegmentStatus(index int, status SegmentStatus) {
	m.segmentStatusMu.Lock()
	oldStatus := m.segmentStatus[index]

	if oldStatus != status {
		m.segmentStatus[index] = status
		m.segmentStatusMu.Unlock()

		// Notify subscribers in a goroutine to avoid blocking
		go m.notifySubscribers(SegmentStatusUpdate{
			SegmentID: index,
			OldStatus: oldStatus,
			NewStatus: status,
		})
	} else {
		m.segmentStatusMu.Unlock()
	}
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
			m.notifySubscribers(SegmentStatusUpdate{
				SegmentID: -1, // Indicates a range update
				OldStatus: SegmentStatusNew, // Not meaningful for range updates
				NewStatus: status,
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

// InitializeSegmentStatus initializes the status for all segments
func (m *Monitor) InitializeSegmentStatus(total int) {
	m.segmentStatusMu.Lock()
	defer m.segmentStatusMu.Unlock()
	m.segmentStatus = make(map[int]SegmentStatus, total)
	for i := 0; i < total; i++ {
		m.segmentStatus[i] = SegmentStatusNew
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
			result += "•"
		case SegmentStatusError:
			result += "!"
		default:
			result += "–"
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
