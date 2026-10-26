package sched

import (
	"fmt"
	"testing"
	"time"

	"github.com/scrap-heap/backend/internal/proto"
)

var fakeStart = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func testTask(id uint32) proto.Task {
	t := proto.BenchTile()
	t.TaskID = id
	return t
}

func startTiles(s *Scheduler, now time.Time, jobID uint32, costs ...float64) []Action {
	tiles := make([]Tile, len(costs))
	for i, cost := range costs {
		tiles[i] = Tile{Task: testTask(uint32(i)), EstimatedCost: cost}
	}
	return s.Handle(StartJob{Now: now, JobID: jobID, Tiles: tiles})
}

func assignments(actions []Action) []AssignTask {
	out := make([]AssignTask, 0)
	for _, action := range actions {
		if a, ok := action.(AssignTask); ok {
			out = append(out, a)
		}
	}
	return out
}

func hasAction[T Action](actions []Action) bool {
	for _, action := range actions {
		if _, ok := action.(T); ok {
			return true
		}
	}
	return false
}

func firstAssignment(t *testing.T, actions []Action) AssignTask {
	t.Helper()
	got := assignments(actions)
	if len(got) != 1 {
		t.Fatalf("got %d assignments, want 1: %#v", len(got), actions)
	}
	return got[0]
}

func join(s *Scheduler, now time.Time, id string, threads int, speed float64) []Action {
	return s.Handle(DeviceJoined{Now: now, DeviceID: id, Threads: threads, Speed: speed})
}

func ask(s *Scheduler, now time.Time, deviceID string, threadID int) []Action {
	return s.Handle(TaskRequested{Now: now, DeviceID: deviceID, ThreadID: threadID})
}

func result(s *Scheduler, now time.Time, assigned AssignTask, jobID uint32) []Action {
	return s.Handle(ResultReceived{Now: now, DeviceID: assigned.DeviceID, JobID: jobID, TaskID: assigned.Task.TaskID, Attempt: assigned.Attempt, ComputeMs: 10})
}

func TestPlainPullCompletesEachTileAndSignalsJobDoneOnce(t *testing.T) {
	s := New(PlainPull)
	startTiles(s, fakeStart, 4, 1, 1)
	join(s, fakeStart, "worker", 1, 1)
	first := firstAssignment(t, ask(s, fakeStart, "worker", 0))
	if first.Task.TaskID != 0 || first.Attempt != 1 {
		t.Fatalf("first assignment = task %d attempt %d", first.Task.TaskID, first.Attempt)
	}
	if actions := ask(s, fakeStart, "worker", 0); len(actions) != 0 {
		t.Fatalf("busy thread request was not ignored: %#v", actions)
	}
	if actions := result(s, fakeStart, first, 4); len(actions) != 0 {
		t.Fatalf("first result unexpectedly finished job: %#v", actions)
	}
	second := firstAssignment(t, ask(s, fakeStart, "worker", 0))
	if second.Task.TaskID != 1 {
		t.Fatalf("plain pull task id = %d, want 1", second.Task.TaskID)
	}
	finished := result(s, fakeStart, second, 4)
	if !hasAction[JobDone](finished) || !hasAction[DropRequests](finished) {
		t.Fatalf("last result actions = %#v, want DropRequests and JobDone", finished)
	}
	if actions := result(s, fakeStart, second, 4); !hasAction[DuplicateDropped](actions) || hasAction[JobDone](actions) {
		t.Fatalf("late duplicate actions = %#v", actions)
	}
}

func TestLeaseExpiryRequeuesAtFrontAndIncrementsAttempt(t *testing.T) {
	s := New(PlainPull)
	startTiles(s, fakeStart, 2, 1, 1)
	join(s, fakeStart, "worker", 1, 1)
	assigned := firstAssignment(t, ask(s, fakeStart, "worker", 0))
	if !assigned.LeaseUntil.Equal(fakeStart.Add(3 * time.Second)) {
		t.Fatalf("leaseUntil=%v, want 3 seconds", assigned.LeaseUntil)
	}
	expired := s.Handle(Tick{Now: assigned.LeaseUntil})
	if !hasAction[Requeued](expired) {
		t.Fatalf("expiry actions = %#v, want Requeued", expired)
	}
	retry := firstAssignment(t, ask(s, assigned.LeaseUntil, "worker", 0))
	if retry.Task.TaskID != assigned.Task.TaskID || retry.Attempt != assigned.Attempt+1 {
		t.Fatalf("retry = task %d attempt %d, want task %d attempt %d", retry.Task.TaskID, retry.Attempt, assigned.Task.TaskID, assigned.Attempt+1)
	}
}

func TestLeaseHasTwoSecondFloor(t *testing.T) {
	s := New(PlainPull)
	startTiles(s, fakeStart, 13, 0.01)
	join(s, fakeStart, "fast", 1, 100)
	got := firstAssignment(t, ask(s, fakeStart, "fast", 0))
	if !got.LeaseUntil.Equal(fakeStart.Add(2 * time.Second)) {
		t.Fatalf("leaseUntil=%v, want two-second minimum", got.LeaseUntil)
	}
}

func TestDisconnectRequeuesAndLateAttemptIsDropped(t *testing.T) {
	s := New(PlainPull)
	startTiles(s, fakeStart, 3, 1)
	join(s, fakeStart, "gone", 1, 1)
	old := firstAssignment(t, ask(s, fakeStart, "gone", 0))
	if actions := s.Handle(DeviceLeft{Now: fakeStart, DeviceID: "gone"}); !hasAction[Requeued](actions) {
		t.Fatalf("disconnect actions = %#v, want Requeued", actions)
	}
	join(s, fakeStart, "replacement", 1, 1)
	current := firstAssignment(t, ask(s, fakeStart, "replacement", 0))
	if current.Attempt != old.Attempt+1 {
		t.Fatalf("new attempt=%d, want %d", current.Attempt, old.Attempt+1)
	}
	late := result(s, fakeStart, old, 3)
	if !hasAction[DuplicateDropped](late) {
		t.Fatalf("late dead-device result actions = %#v", late)
	}
	stale := result(s, fakeStart, current, 99)
	if !hasAction[StaleDropped](stale) {
		t.Fatalf("stale-job result actions = %#v", stale)
	}
}

func TestDuplicateAndStaleResultsAreCounted(t *testing.T) {
	s := New(PlainPull)
	startTiles(s, fakeStart, 5, 1)
	join(s, fakeStart, "worker", 1, 1)
	assigned := firstAssignment(t, ask(s, fakeStart, "worker", 0))
	if actions := result(s, fakeStart, assigned, 77); !hasAction[StaleDropped](actions) {
		t.Fatalf("stale result actions = %#v", actions)
	}
	if actions := result(s, fakeStart, assigned, 5); !hasAction[JobDone](actions) {
		t.Fatalf("valid final result actions = %#v", actions)
	}
	if actions := result(s, fakeStart, assigned, 5); !hasAction[DuplicateDropped](actions) {
		t.Fatalf("duplicate result actions = %#v", actions)
	}
}

func TestHeldRequestIsAnsweredWhenTileIsRequeued(t *testing.T) {
	// A request can be held before a job exists. The private setup below also
	// covers dispatching a pending request on a lease requeue; normal public
	// flow usually end-game duplicates instead of holding while work is leased.
	s := New(PlainPull)
	join(s, fakeStart, "a", 1, 1)
	join(s, fakeStart, "b", 1, 1)
	ask(s, fakeStart, "b", 0)
	started := startTiles(s, fakeStart, 6, 1)
	preJobHeld := firstAssignment(t, started)
	if preJobHeld.DeviceID != "b" {
		t.Fatalf("pre-job held request went to %q", preJobHeld.DeviceID)
	}
	initial := firstAssignment(t, ask(s, fakeStart, "a", 0))
	// Put b's request back into pending state to exercise the requeue drain path.
	s.devices["b"].slots[0] = slot{pending: true}
	s.held = []request{{deviceID: "b", threadID: 0}}
	if actions := s.Handle(Tick{Now: initial.LeaseUntil}); len(assignments(actions)) != 1 {
		t.Fatalf("requeue did not answer held request: %#v", actions)
	} else if got := assignments(actions)[0]; got.DeviceID != "b" || got.Attempt != initial.Attempt+1 {
		t.Fatalf("held assignment = %#v, want b with incremented attempt", got)
	}
}

func TestRequestCapIgnoresExtraRequests(t *testing.T) {
	s := New(PlainPull)
	startTiles(s, fakeStart, 7, 1, 1, 1)
	join(s, fakeStart, "worker", 2, 1)
	firstAssignment(t, ask(s, fakeStart, "worker", 0))
	firstAssignment(t, ask(s, fakeStart, "worker", 1))
	if actions := ask(s, fakeStart, "worker", 2); len(actions) != 0 {
		t.Fatalf("extra request returned actions: %#v", actions)
	}
	if len(s.held) != 0 || len(s.devices["worker"].slots) != 2 {
		t.Fatalf("extra request consumed another slot: held=%d slots=%d", len(s.held), len(s.devices["worker"].slots))
	}
}

func TestCapabilityAwareSoftAffinity(t *testing.T) {
	s := New(CapabilityAware)
	startTiles(s, fakeStart, 8, 1, 2, 3, 4, 5)
	join(s, fakeStart, "strong", 1, 10)
	join(s, fakeStart, "weak", 4, 1)
	strong := firstAssignment(t, ask(s, fakeStart, "strong", 0))
	if strong.Task.TaskID != 4 {
		t.Fatalf("strong got task %d, want heaviest task 4", strong.Task.TaskID)
	}
	wantWeak := []uint32{2, 1, 0, 3}
	for thread, wantID := range wantWeak {
		assigned := firstAssignment(t, ask(s, fakeStart, "weak", thread))
		if assigned.Task.TaskID != wantID {
			t.Fatalf("weak thread %d got task %d, want %d", thread, assigned.Task.TaskID, wantID)
		}
	}
}

func TestSingleDeviceIsStrong(t *testing.T) {
	s := New(CapabilityAware)
	startTiles(s, fakeStart, 9, 1, 2, 3)
	join(s, fakeStart, "only", 1, 1)
	got := firstAssignment(t, ask(s, fakeStart, "only", 0))
	if got.Task.TaskID != 2 {
		t.Fatalf("single device got task %d, want heaviest 2", got.Task.TaskID)
	}
}

func TestEndGameDuplicatesOldestLeasedTileFirstResultWins(t *testing.T) {
	s := New(PlainPull)
	startTiles(s, fakeStart, 10, 1, 1)
	join(s, fakeStart, "first", 2, 1)
	join(s, fakeStart, "second", 1, 1)
	first := firstAssignment(t, ask(s, fakeStart, "first", 0))
	second := firstAssignment(t, ask(s, fakeStart, "first", 1))
	duplicate := firstAssignment(t, ask(s, fakeStart, "second", 0))
	if duplicate.Task.TaskID != first.Task.TaskID || duplicate.Attempt != first.Attempt+1 || second.Task.TaskID != 1 {
		t.Fatalf("end-game assignments first=%#v second=%#v duplicate=%#v", first, second, duplicate)
	}
	if actions := result(s, fakeStart, duplicate, 10); hasAction[JobDone](actions) {
		t.Fatalf("duplicate winner finished too soon: %#v", actions)
	}
	if actions := result(s, fakeStart, second, 10); !hasAction[JobDone](actions) {
		t.Fatalf("last valid result did not finish job: %#v", actions)
	}
	if actions := result(s, fakeStart, first, 10); !hasAction[DuplicateDropped](actions) {
		t.Fatalf("other attempt was not dropped: %#v", actions)
	}
}

func TestAttemptDoesNotWrapPastUint16(t *testing.T) {
	s := New(PlainPull)
	startTiles(s, fakeStart, 12, 1)
	join(s, fakeStart, "worker", 1, 1)
	s.tiles[0].nextAttempt = 65534
	last := firstAssignment(t, ask(s, fakeStart, "worker", 0))
	if last.Attempt != 65535 {
		t.Fatalf("last representable attempt=%d", last.Attempt)
	}
	if actions := s.Handle(Tick{Now: last.LeaseUntil}); !hasAction[Requeued](actions) {
		t.Fatalf("expiry actions=%#v", actions)
	}
	if actions := ask(s, last.LeaseUntil, "worker", 0); len(assignments(actions)) != 0 || len(s.held) != 1 {
		t.Fatalf("attempt wrapped or pending request lost: actions=%#v held=%d", actions, len(s.held))
	}
}

func TestResetRequeuesLeasedAndDropsRequests(t *testing.T) {
	s := New(PlainPull)
	startTiles(s, fakeStart, 11, 1, 1)
	join(s, fakeStart, "worker", 2, 1)
	assigned := firstAssignment(t, ask(s, fakeStart, "worker", 0))
	actions := s.Handle(Reset{Now: fakeStart.Add(time.Second)})
	if !hasAction[DropRequests](actions) || !hasAction[Requeued](actions) {
		t.Fatalf("reset actions = %#v", actions)
	}
	retry := firstAssignment(t, ask(s, fakeStart.Add(time.Second), "worker", 0))
	if retry.Task.TaskID != assigned.Task.TaskID || retry.Attempt != 2 {
		t.Fatalf("reset retry = %#v", retry)
	}
}

func TestDeterministicThreeDeviceSimulationCompletes1024Tiles(t *testing.T) {
	s := New(CapabilityAware)
	tiles := make([]Tile, proto.CanvasSize/proto.TileSize*(proto.CanvasSize/proto.TileSize))
	for i := range tiles {
		task := proto.BenchTile()
		task.TaskID = uint32(i)
		tiles[i] = Tile{Task: task, EstimatedCost: float64(1 + i%17)}
	}
	s.Handle(StartJob{Now: fakeStart, JobID: 42, Tiles: tiles})
	devices := []struct {
		id      string
		threads int
		speed   float64
	}{{"slow", 2, 1}, {"mid", 2, 4}, {"fast", 2, 9}}
	for _, d := range devices {
		join(s, fakeStart, d.id, d.threads, d.speed)
	}
	work := make([]AssignTask, 0, 16)
	for _, d := range devices {
		for thread := 0; thread < d.threads; thread++ {
			work = append(work, assignments(ask(s, fakeStart, d.id, thread))...)
		}
	}
	completed := make(map[uint32]AssignTask, len(tiles))
	jobDoneCount := 0
	for len(work) > 0 && jobDoneCount == 0 {
		current := work[0]
		work = work[1:]
		actions := result(s, fakeStart, current, 42)
		if hasAction[DuplicateDropped](actions) {
			work = append(work, assignments(ask(s, fakeStart, current.DeviceID, current.ThreadID))...)
			continue
		}
		if _, exists := completed[current.Task.TaskID]; !exists {
			completed[current.Task.TaskID] = current
		}
		for _, action := range actions {
			if _, ok := action.(JobDone); ok {
				jobDoneCount++
			}
		}
		work = append(work, assignments(ask(s, fakeStart, current.DeviceID, current.ThreadID))...)
	}
	if len(completed) != len(tiles) || jobDoneCount != 1 {
		t.Fatalf("simulation completed %d/%d tiles; JobDone count=%d, queued work=%d", len(completed), len(tiles), jobDoneCount, len(work))
	}
	for _, prior := range completed {
		actions := result(s, fakeStart, prior, 42)
		if !hasAction[DuplicateDropped](actions) {
			t.Fatalf("completed tile %d did not drop duplicate", prior.Task.TaskID)
		}
		for _, action := range actions {
			if _, ok := action.(JobDone); ok {
				t.Fatal("JobDone fired more than once")
			}
		}
	}
	if len(completed) != 1024 {
		t.Fatal(fmt.Sprintf("completion count=%d", len(completed)))
	}
}
