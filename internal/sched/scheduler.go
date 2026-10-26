// Package sched implements a pure, event-driven tile scheduler. It performs no
// I/O, starts no goroutines, and never reads wall-clock time. Callers send an
// event with its Now value and must own a Scheduler from one goroutine. Device
// speed is measured in estimated-cost units per second; callers should derive it
// as benchTilesPerSec * benchTileCost.
package sched

import (
	"math"
	"sort"
	"time"

	"github.com/scrap-heap/backend/internal/proto"
)

// Mode selects the queue policy used for a job.
type Mode uint8

const (
	PlainPull Mode = iota
	CapabilityAware
)

// Tile pairs render parameters with the scheduler's estimated work cost.
type Tile struct {
	Task          proto.Task
	EstimatedCost float64
}

// Event is an input to Scheduler.Handle. All events carry their processing time.
type Event interface {
	eventTime() time.Time
	isEvent()
}

// StartJob replaces the current tile set. A pending request can be answered by
// the new job; use Reset first when the caller wants all requests dropped.
type StartJob struct {
	Now   time.Time
	JobID uint32
	Tiles []Tile
}

func (e StartJob) eventTime() time.Time { return e.Now }
func (StartJob) isEvent()               {}

// DeviceJoined registers a worker. Speed is estimated-cost units per second.
type DeviceJoined struct {
	Now      time.Time
	DeviceID string
	Threads  int
	Speed    float64
}

func (e DeviceJoined) eventTime() time.Time { return e.Now }
func (DeviceJoined) isEvent()               {}

// DeviceLeft removes a worker and immediately releases its leases and requests.
type DeviceLeft struct {
	Now      time.Time
	DeviceID string
}

func (e DeviceLeft) eventTime() time.Time { return e.Now }
func (DeviceLeft) isEvent()               {}

// TaskRequested asks for one tile on a worker thread.
type TaskRequested struct {
	Now      time.Time
	DeviceID string
	ThreadID int
}

func (e TaskRequested) eventTime() time.Time { return e.Now }
func (TaskRequested) isEvent()               {}

// ResultReceived reports a completed attempt. Invalid compute times are ignored.
type ResultReceived struct {
	Now       time.Time
	DeviceID  string
	JobID     uint32
	TaskID    uint32
	Attempt   uint16
	ComputeMs float32
}

func (e ResultReceived) eventTime() time.Time { return e.Now }
func (ResultReceived) isEvent()               {}

// Tick expires leases whose deadline is at or before Now.
type Tick struct{ Now time.Time }

func (e Tick) eventTime() time.Time { return e.Now }
func (Tick) isEvent()               {}

// Reset requeues leased tiles and drops all outstanding pull requests.
type Reset struct{ Now time.Time }

func (e Reset) eventTime() time.Time { return e.Now }
func (Reset) isEvent()               {}

// Action is an output for the caller to perform or record.
type Action interface{ isAction() }

// AssignTask sends a tile assignment to a particular worker thread.
type AssignTask struct {
	DeviceID   string
	ThreadID   int
	Task       proto.Task
	Attempt    uint16
	LeaseUntil time.Time
}

func (AssignTask) isAction() {}

// JobDone signals that every tile in the active job has completed.
type JobDone struct{ JobID uint32 }

func (JobDone) isAction() {}

// DropRequests asks the caller to clear any requests it has retained externally.
type DropRequests struct{ JobID uint32 }

func (DropRequests) isAction() {}

// RequeueReason records why an assignment was returned to its queue.
type RequeueReason string

const (
	LeaseExpired       RequeueReason = "lease_expired"
	DeviceDisconnected RequeueReason = "device_disconnected"
	SchedulerReset     RequeueReason = "reset"
)

// Requeued is a metric event for a tile returned to a queue.
type Requeued struct {
	JobID   uint32
	TaskID  uint32
	Attempt uint16
	Reason  RequeueReason
}

func (Requeued) isAction() {}

// DuplicateDropped is a metric event for an invalid or late duplicate result.
type DuplicateDropped struct {
	JobID   uint32
	TaskID  uint32
	Attempt uint16
}

func (DuplicateDropped) isAction() {}

// StaleDropped is a metric event for a result from a non-current job.
type StaleDropped struct {
	JobID   uint32
	TaskID  uint32
	Attempt uint16
}

func (StaleDropped) isAction() {}

type tileStatus uint8

const (
	queued tileStatus = iota
	leased
	done
)

type queueKind uint8

const (
	plainQueue queueKind = iota
	heavyQueue
	lightQueue
)

type tileState struct {
	task        proto.Task
	cost        float64
	status      tileStatus
	queue       queueKind
	nextAttempt uint32
	leases      map[uint16]*lease
}

type lease struct {
	deviceID string
	threadID int
	attempt  uint16
	until    time.Time
	order    uint64
}

type slot struct {
	pending bool
	taskID  uint32
	attempt uint16
}

type device struct {
	threads int
	speed   float64
	slots   map[int]slot
}

type request struct {
	deviceID string
	threadID int
}

// Scheduler is an event-driven state machine and is not safe for concurrent use.
type Scheduler struct {
	mode       Mode
	jobID      uint32
	hasJob     bool
	tiles      map[uint32]*tileState
	plain      []uint32
	heavy      []uint32
	light      []uint32
	devices    map[string]*device
	held       []request
	leaseOrder uint64
	doneCount  int
	jobDone    bool
}

// New constructs a scheduler with the requested queue policy.
func New(mode Mode) *Scheduler {
	if mode != CapabilityAware {
		mode = PlainPull
	}
	return &Scheduler{mode: mode, tiles: make(map[uint32]*tileState), devices: make(map[string]*device)}
}

// Handle applies one event and returns ordered actions. Time is read only from ev.Now.
func (s *Scheduler) Handle(ev Event) []Action {
	if ev == nil {
		return nil
	}
	now := ev.eventTime()
	switch e := ev.(type) {
	case StartJob:
		return s.startJob(e, now)
	case DeviceJoined:
		return s.deviceJoined(e, now)
	case DeviceLeft:
		return s.deviceLeft(e.DeviceID, DeviceDisconnected, now)
	case TaskRequested:
		return s.taskRequested(e)
	case ResultReceived:
		return s.resultReceived(e)
	case Tick:
		return s.tick(now)
	case Reset:
		return s.reset()
	default:
		return nil
	}
}

func (s *Scheduler) startJob(e StartJob, now time.Time) []Action {
	// A new job invalidates old leases but preserves requests waiting for work.
	for _, d := range s.devices {
		for threadID, sl := range d.slots {
			if !sl.pending {
				delete(d.slots, threadID)
			}
		}
	}
	s.jobID, s.hasJob = e.JobID, true
	s.leaseOrder = 0
	s.tiles = make(map[uint32]*tileState)
	s.plain, s.heavy, s.light = nil, nil, nil
	s.doneCount, s.jobDone = 0, false

	costs := make([]float64, 0, len(e.Tiles))
	for _, input := range e.Tiles {
		if _, exists := s.tiles[input.Task.TaskID]; exists || !validCost(input.EstimatedCost) || !validTask(input.Task) {
			continue
		}
		task := input.Task
		task.JobID = e.JobID
		task.Attempt = 0
		state := &tileState{task: task, cost: input.EstimatedCost, status: queued, leases: make(map[uint16]*lease)}
		s.tiles[task.TaskID] = state
		s.plain = append(s.plain, task.TaskID)
		costs = append(costs, input.EstimatedCost)
	}
	if s.mode == CapabilityAware && len(costs) > 0 {
		median := medianFloat64(costs)
		ids := make([]uint32, 0, len(s.tiles))
		for id := range s.tiles {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		s.plain = nil
		for _, id := range ids {
			t := s.tiles[id]
			if t.cost > median {
				t.queue = heavyQueue
				s.heavy = append(s.heavy, id)
			} else {
				t.queue = lightQueue
				s.light = append(s.light, id)
			}
		}
		s.sortQueueByCost(s.heavy)
		s.sortQueueByCost(s.light)
	} else {
		for _, id := range s.plain {
			s.tiles[id].queue = plainQueue
		}
	}

	actions := s.dispatchHeld(now)
	if len(s.tiles) == 0 {
		actions = append(actions, s.finishJob()...)
	}
	return actions
}

func (s *Scheduler) deviceJoined(e DeviceJoined, now time.Time) []Action {
	var actions []Action
	if _, exists := s.devices[e.DeviceID]; exists {
		actions = append(actions, s.deviceLeft(e.DeviceID, DeviceDisconnected, now)...)
	}
	if e.DeviceID == "" || e.Threads <= 0 || !finite(e.Speed) || e.Speed <= 0 {
		return actions
	}
	s.devices[e.DeviceID] = &device{threads: e.Threads, speed: e.Speed, slots: make(map[int]slot)}
	actions = append(actions, s.dispatchHeld(now)...)
	return actions
}

func (s *Scheduler) deviceLeft(deviceID string, reason RequeueReason, now time.Time) []Action {
	d := s.devices[deviceID]
	if d == nil {
		return nil
	}
	for i := range s.held {
		if s.held[i].deviceID == deviceID {
			s.held[i] = request{}
		}
	}
	s.compactHeld()
	leases := s.leasesForDevice(deviceID)
	var actions []Action
	for _, item := range leases {
		t := s.tiles[item.taskID]
		if t == nil || t.status != leased || t.leases[item.l.attempt] == nil {
			continue
		}
		s.removeLease(t, item.taskID, item.l)
		if len(t.leases) == 0 {
			actions = append(actions, s.requeue(item.taskID, t, item.l.attempt, reason)...)
		}
	}
	delete(s.devices, deviceID)
	actions = append(actions, s.dispatchHeld(now)...)
	return actions
}

func (s *Scheduler) taskRequested(e TaskRequested) []Action {
	d := s.devices[e.DeviceID]
	if d == nil || e.ThreadID < 0 || e.ThreadID >= d.threads || len(d.slots) >= d.threads {
		return nil
	}
	if _, busy := d.slots[e.ThreadID]; busy {
		return nil
	}
	taskID, t := s.nextFor(d)
	if t != nil {
		return []Action{s.assign(e.DeviceID, e.ThreadID, taskID, t, e.Now)}
	}
	if s.jobDone {
		return nil
	}
	d.slots[e.ThreadID] = slot{pending: true}
	s.held = append(s.held, request{deviceID: e.DeviceID, threadID: e.ThreadID})
	return nil
}

func (s *Scheduler) resultReceived(e ResultReceived) []Action {
	if !s.hasJob || e.JobID != s.jobID {
		return []Action{StaleDropped{JobID: e.JobID, TaskID: e.TaskID, Attempt: e.Attempt}}
	}
	t := s.tiles[e.TaskID]
	if t == nil || t.status != leased || !finite32(e.ComputeMs) || e.ComputeMs < 0 {
		return []Action{DuplicateDropped{JobID: e.JobID, TaskID: e.TaskID, Attempt: e.Attempt}}
	}
	l := t.leases[e.Attempt]
	if l == nil || l.deviceID != e.DeviceID {
		return []Action{DuplicateDropped{JobID: e.JobID, TaskID: e.TaskID, Attempt: e.Attempt}}
	}
	for _, active := range t.leases {
		s.removeLease(t, e.TaskID, active)
	}
	t.status = done
	s.doneCount++
	if s.doneCount == len(s.tiles) && !s.jobDone {
		return s.finishJob()
	}
	return nil
}

func (s *Scheduler) tick(now time.Time) []Action {
	var expired []struct {
		taskID uint32
		lease  *lease
	}
	for taskID, t := range s.tiles {
		if t.status != leased {
			continue
		}
		for _, l := range t.leases {
			if !l.until.After(now) {
				expired = append(expired, struct {
					taskID uint32
					lease  *lease
				}{taskID, l})
			}
		}
	}
	sort.Slice(expired, func(i, j int) bool { return expired[i].lease.order < expired[j].lease.order })
	var actions []Action
	for _, item := range expired {
		t := s.tiles[item.taskID]
		if t == nil || t.leases[item.lease.attempt] != item.lease {
			continue
		}
		s.removeLease(t, item.taskID, item.lease)
		if len(t.leases) == 0 {
			actions = append(actions, s.requeue(item.taskID, t, item.lease.attempt, LeaseExpired)...)
		}
	}
	actions = append(actions, s.dispatchHeld(now)...)
	return actions
}

func (s *Scheduler) reset() []Action {
	var requeueIDs []uint32
	attempts := make(map[uint32]uint16)
	for id, t := range s.tiles {
		if t.status == leased {
			var oldest *lease
			for _, l := range t.leases {
				if oldest == nil || l.order < oldest.order {
					oldest = l
				}
			}
			if oldest != nil {
				requeueIDs = append(requeueIDs, id)
				attempts[id] = oldest.attempt
			}
		}
	}
	sort.Slice(requeueIDs, func(i, j int) bool { return requeueIDs[i] < requeueIDs[j] })
	for _, d := range s.devices {
		for threadID := range d.slots {
			delete(d.slots, threadID)
		}
	}
	s.held = nil
	var actions []Action
	for i := len(requeueIDs) - 1; i >= 0; i-- {
		id := requeueIDs[i]
		t := s.tiles[id]
		for _, l := range t.leases {
			delete(t.leases, l.attempt)
		}
		t.status = queued
		t.task.Attempt = 0
		s.pushFront(id, t.queue)
		actions = append(actions, Requeued{JobID: s.jobID, TaskID: id, Attempt: attempts[id], Reason: SchedulerReset})
	}
	actions = append(actions, DropRequests{JobID: s.jobID})
	return actions
}

func (s *Scheduler) finishJob() []Action {
	if s.jobDone {
		return nil
	}
	s.jobDone = true
	for _, d := range s.devices {
		for threadID, sl := range d.slots {
			if sl.pending {
				delete(d.slots, threadID)
			}
		}
	}
	s.held = nil
	return []Action{DropRequests{JobID: s.jobID}, JobDone{JobID: s.jobID}}
}

func (s *Scheduler) dispatchHeld(now time.Time) []Action {
	var actions []Action
	for len(s.held) > 0 {
		r := s.held[0]
		s.held = s.held[1:]
		d := s.devices[r.deviceID]
		if d == nil {
			continue
		}
		sl, ok := d.slots[r.threadID]
		if !ok || !sl.pending {
			continue
		}
		taskID, t := s.nextFor(d)
		if t == nil {
			s.held = append([]request{r}, s.held...)
			break
		}
		actions = append(actions, s.assign(r.deviceID, r.threadID, taskID, t, now))
	}
	return actions
}

func (s *Scheduler) nextFor(d *device) (uint32, *tileState) {
	var id uint32
	var ok bool
	if s.mode == PlainPull {
		id, _, ok = s.popQueued(plainQueue, false)
	} else if s.isStrong(d.speed) {
		id, _, ok = s.popQueued(heavyQueue, false)
		if !ok {
			id, _, ok = s.popQueued(lightQueue, false)
		}
	} else {
		id, _, ok = s.popQueued(lightQueue, false)
		if !ok {
			id, _, ok = s.popQueued(heavyQueue, true)
		}
	}
	if ok {
		return id, s.tiles[id]
	}
	if taskID, t := s.oldestLeased(); t != nil {
		return taskID, t
	}
	return 0, nil
}

func (s *Scheduler) assign(deviceID string, threadID int, taskID uint32, t *tileState, now time.Time) AssignTask {
	t.nextAttempt++
	attempt := uint16(t.nextAttempt)
	l := &lease{deviceID: deviceID, threadID: threadID, attempt: attempt, until: now.Add(leaseDuration(t.cost, s.devices[deviceID].speed)), order: s.nextLeaseOrder()}
	t.leases[attempt] = l
	t.status = leased
	t.task.JobID, t.task.Attempt = s.jobID, attempt
	s.devices[deviceID].slots[threadID] = slot{taskID: taskID, attempt: attempt}
	return AssignTask{DeviceID: deviceID, ThreadID: threadID, Task: t.task, Attempt: attempt, LeaseUntil: l.until}
}

func (s *Scheduler) requeue(taskID uint32, t *tileState, attempt uint16, reason RequeueReason) []Action {
	t.status = queued
	t.task.Attempt = 0
	s.pushFront(taskID, t.queue)
	return []Action{Requeued{JobID: s.jobID, TaskID: taskID, Attempt: attempt, Reason: reason}}
}

func (s *Scheduler) oldestLeased() (uint32, *tileState) {
	var selectedID uint32
	var selected *tileState
	var oldest uint64
	for id, t := range s.tiles {
		if t.status != leased || t.nextAttempt >= math.MaxUint16 {
			continue
		}
		for _, l := range t.leases {
			if selected == nil || l.order < oldest {
				selectedID, selected, oldest = id, t, l.order
			}
		}
	}
	return selectedID, selected
}

// popQueued removes the next assignable queued tile without wrapping attempts.
func (s *Scheduler) popQueued(kind queueKind, fromBack bool) (uint32, *tileState, bool) {
	var ids *[]uint32
	switch kind {
	case heavyQueue:
		ids = &s.heavy
	case lightQueue:
		ids = &s.light
	default:
		ids = &s.plain
	}
	for offset := 0; offset < len(*ids); offset++ {
		index := offset
		if fromBack {
			index = len(*ids) - 1 - offset
		}
		id := (*ids)[index]
		t := s.tiles[id]
		if t == nil || t.status != queued || t.nextAttempt >= math.MaxUint16 {
			continue
		}
		copy((*ids)[index:], (*ids)[index+1:])
		*ids = (*ids)[:len(*ids)-1]
		return id, t, true
	}
	return 0, nil, false
}

func (s *Scheduler) isStrong(speed float64) bool {
	if len(s.devices) <= 1 {
		return true
	}
	speeds := make([]float64, 0, len(s.devices))
	for _, d := range s.devices {
		speeds = append(speeds, d.speed)
	}
	return speed >= medianFloat64(speeds)
}

func (s *Scheduler) sortQueueByCost(ids []uint32) {
	sort.SliceStable(ids, func(i, j int) bool {
		return s.tiles[ids[i]].cost > s.tiles[ids[j]].cost
	})
}

func (s *Scheduler) pushFront(id uint32, kind queueKind) {
	switch kind {
	case heavyQueue:
		s.heavy = append(s.heavy, 0)
		copy(s.heavy[1:], s.heavy[:len(s.heavy)-1])
		s.heavy[0] = id
	case lightQueue:
		s.light = append(s.light, 0)
		copy(s.light[1:], s.light[:len(s.light)-1])
		s.light[0] = id
	default:
		s.plain = append(s.plain, 0)
		copy(s.plain[1:], s.plain[:len(s.plain)-1])
		s.plain[0] = id
	}
}

func (s *Scheduler) removeLease(t *tileState, taskID uint32, l *lease) {
	delete(t.leases, l.attempt)
	if d := s.devices[l.deviceID]; d != nil {
		if sl, ok := d.slots[l.threadID]; ok && !sl.pending && sl.taskID == taskID && sl.attempt == l.attempt {
			delete(d.slots, l.threadID)
		}
	}
}

func (s *Scheduler) leasesForDevice(deviceID string) []struct {
	taskID uint32
	l      *lease
} {
	var out []struct {
		taskID uint32
		l      *lease
	}
	for id, t := range s.tiles {
		for _, l := range t.leases {
			if l.deviceID == deviceID {
				out = append(out, struct {
					taskID uint32
					l      *lease
				}{id, l})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].l.order < out[j].l.order })
	return out
}

func (s *Scheduler) compactHeld() {
	out := s.held[:0]
	for _, r := range s.held {
		if r.deviceID != "" {
			out = append(out, r)
		}
	}
	s.held = out
}

func (s *Scheduler) nextLeaseOrder() uint64 {
	s.leaseOrder++
	return s.leaseOrder
}

func leaseDuration(cost, speed float64) time.Duration {
	seconds := 3 * cost / speed
	maxSeconds := float64(math.MaxInt64) / float64(time.Second)
	var d time.Duration
	if !finite(seconds) || seconds >= maxSeconds {
		d = time.Duration(math.MaxInt64)
	} else {
		d = time.Duration(seconds * float64(time.Second))
	}
	if d < 2*time.Second {
		return 2 * time.Second
	}
	return d
}

func validCost(v float64) bool { return finite(v) && v >= 0 }

func validTask(task proto.Task) bool {
	return task.W > 0 && task.H > 0 && task.W <= proto.MaxTileDim && task.H <= proto.MaxTileDim &&
		task.MaxIter > 0 && finite(task.X0) && finite(task.Y0) && finite(task.Scale) && task.Scale > 0
}

func medianFloat64(values []float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return sorted[mid-1]/2 + sorted[mid]/2
}

func popFront(ids []uint32) (uint32, []uint32, bool) {
	if len(ids) == 0 {
		return 0, ids, false
	}
	return ids[0], ids[1:], true
}

func popBack(ids []uint32) (uint32, []uint32, bool) {
	if len(ids) == 0 {
		return 0, ids, false
	}
	return ids[len(ids)-1], ids[:len(ids)-1], true
}

func finite(v float64) bool   { return !math.IsNaN(v) && !math.IsInf(v, 0) }
func finite32(v float32) bool { return finite(float64(v)) }
