// Copyright 2021 Chaos Mesh Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package bpm

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/chaos-mesh/chaos-mesh/pkg/log"
)

func RandomeIdentifier() string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, 10)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func WaitProcess(m *BackgroundProcessManager, proc *Process, exceedTime time.Duration) {
	timeExceed := false
	select {
	case <-proc.Stopped():
	case <-time.Tick(exceedTime):
		timeExceed = true
	}
	Expect(timeExceed).To(BeFalse())
}

var _ = Describe("background process manager", func() {
	log, err := log.NewDefaultZapLogger()
	Expect(err).To(BeNil())
	m := StartBackgroundProcessManager(nil, log)

	Context("normally exited process", func() {
		It("should work", func() {
			cmd := DefaultProcessBuilder("sleep", "2").Build(context.Background())
			p, err := m.StartProcess(context.Background(), cmd)
			Expect(err).To(BeNil())

			WaitProcess(m, p, time.Second*3)
		})

		It("processes with the same identifier", func() {
			identifier := RandomeIdentifier()

			cmd := DefaultProcessBuilder("sleep", "2").
				SetIdentifier(identifier).
				Build(context.Background())
			p1, err := m.StartProcess(context.Background(), cmd)
			Expect(err).To(BeNil())

			// get error
			cmd2 := DefaultProcessBuilder("sleep", "2").
				SetIdentifier(identifier).
				Build(context.Background())
			_, err = m.StartProcess(context.Background(), cmd2)
			Expect(err).NotTo(BeNil())
			Expect(strings.Contains(err.Error(), fmt.Sprintf("process with identifier %s is running", identifier))).To(BeTrue())

			WaitProcess(m, p1, time.Second*3)
			cmd3 := DefaultProcessBuilder("sleep", "2").
				SetIdentifier(identifier).
				Build(context.TODO())
			p3, err := m.StartProcess(context.Background(), cmd3)
			Expect(err).To(BeNil())

			WaitProcess(m, p3, time.Second*3)
		})
	})

	Context("kill process", func() {
		It("should work", func() {
			cmd := DefaultProcessBuilder("sleep", "2").Build(context.Background())
			p, err := m.StartProcess(context.Background(), cmd)
			Expect(err).To(BeNil())

			err = m.KillBackgroundProcess(context.Background(), p.Uid)
			Expect(err).To(BeNil())

			WaitProcess(m, p, time.Second*0)
		})

		It("process with the same identifier", func() {
			identifier := RandomeIdentifier()

			cmd := DefaultProcessBuilder("sleep", "2").
				SetIdentifier(identifier).
				Build(context.Background())
			p1, err := m.StartProcess(context.Background(), cmd)
			Expect(err).To(BeNil())

			// get error
			cmd2 := DefaultProcessBuilder("sleep", "2").
				SetIdentifier(identifier).
				Build(context.Background())
			_, err = m.StartProcess(context.Background(), cmd2)
			Expect(err).NotTo(BeNil())
			Expect(strings.Contains(err.Error(), fmt.Sprintf("process with identifier %s is running", identifier))).To(BeTrue())
			WaitProcess(m, p1, time.Second*3)

			cmd3 := DefaultProcessBuilder("sleep", "2").
				SetIdentifier(identifier).
				Build(context.Background())
			p3, err := m.StartProcess(context.Background(), cmd3)
			Expect(err).To(BeNil())

			err = m.KillBackgroundProcess(context.Background(), p3.Uid)
			Expect(err).To(BeNil())

			cmd4 := DefaultProcessBuilder("sleep", "2").
				SetIdentifier(identifier).
				Build(context.Background())
			p4, err := m.StartProcess(context.Background(), cmd4)
			Expect(err).To(BeNil())
			WaitProcess(m, p4, time.Second*3)
		})
	})

	Context("get identifiers", func() {
		It("should work", func() {
			identifier := RandomeIdentifier()
			cmd := DefaultProcessBuilder("sleep", "2").
				SetIdentifier(identifier).
				Build(context.Background())

			p, err := m.StartProcess(context.Background(), cmd)
			Expect(err).To(BeNil())

			ids := m.GetIdentifiers()
			Expect(ids).To(Equal([]string{identifier}))

			WaitProcess(m, p, time.Second*3)

			// wait for deleting identifier
			time.Sleep(time.Second * 2)
			ids = m.GetIdentifiers()
			Expect(len(ids)).To(Equal(0))
		})

		It("should work with nil identifier", func() {
			cmd := DefaultProcessBuilder("sleep", "2").Build(context.Background())

			p, err := m.StartProcess(context.Background(), cmd)
			Expect(err).To(BeNil())

			ids := m.GetIdentifiers()
			Expect(len(ids)).To(Equal(0))

			WaitProcess(m, p, time.Second*5)
		})
	})

	Context("get uid", func() {
		It("kill process", func() {
			cmd := DefaultProcessBuilder("sleep", "2").Build(context.Background())
			p, err := m.StartProcess(context.Background(), cmd)
			Expect(err).To(BeNil())

			uid, loaded := m.GetUID(p.Pair)
			Expect(loaded).To(BeTrue())

			err = m.KillBackgroundProcess(context.Background(), uid)
			Expect(err).To(BeNil())

			WaitProcess(m, p, time.Second*0)
		})
	})

	// Regression test for the HTTPChaos retry bug:
	//
	//   On a retried ApplyHttpChaos gRPC call, the daemon previously
	//   re-invoked StartProcess with the same `tproxy-<containerId>`
	//   identifier, was rejected with "process with identifier ... is
	//   running", and had no way to recover the UID/pipes of the live
	//   tproxy. GetUidByIdentifier closes that gap.
	Context("get uid by identifier", func() {
		It("returns the live UID so ApplyHttpChaos can recover on retry", func() {
			identifier := RandomeIdentifier()

			// First start: succeeds and registers identifier -> uid.
			cmd1 := DefaultProcessBuilder("sleep", "5").
				SetIdentifier(identifier).
				Build(context.Background())
			p1, err := m.StartProcess(context.Background(), cmd1)
			Expect(err).To(BeNil())

			// Look up by identifier -- the regression: must return p1's UID
			// and not be empty (which is what would force createHttpChaos to
			// re-spawn and trip "is running").
			uid, ok := m.GetUidByIdentifier(identifier)
			Expect(ok).To(BeTrue())
			Expect(uid).To(Equal(p1.Uid))

			// Second start with the same identifier still must fail (the
			// no-double-spawn invariant), but the caller now has a way to
			// recover: GetUidByIdentifier still returns the same UID, and
			// GetPipes on it gives back usable pipes.
			cmd2 := DefaultProcessBuilder("sleep", "5").
				SetIdentifier(identifier).
				Build(context.Background())
			_, err = m.StartProcess(context.Background(), cmd2)
			Expect(err).NotTo(BeNil())
			Expect(strings.Contains(err.Error(), "is running")).To(BeTrue())

			uid2, ok2 := m.GetUidByIdentifier(identifier)
			Expect(ok2).To(BeTrue())
			Expect(uid2).To(Equal(uid))

			pipes, ok3 := m.GetPipes(uid2)
			Expect(ok3).To(BeTrue())
			Expect(pipes.Stdin).NotTo(BeNil())
			Expect(pipes.Stdout).NotTo(BeNil())

			// Cleanup so we don't leak the sleep process or its identifier.
			err = m.KillBackgroundProcess(context.Background(), uid)
			Expect(err).To(BeNil())
			WaitProcess(m, p1, time.Second*0)
		})

		It("returns false for an unknown or already-exited identifier", func() {
			_, ok := m.GetUidByIdentifier("never-registered-identifier")
			Expect(ok).To(BeFalse())

			identifier := RandomeIdentifier()
			cmd := DefaultProcessBuilder("sleep", "0").
				SetIdentifier(identifier).
				Build(context.Background())
			p, err := m.StartProcess(context.Background(), cmd)
			Expect(err).To(BeNil())

			WaitProcess(m, p, time.Second*3)

			// Once the process has exited, the death-channel goroutine
			// removes the identifier; lookups must miss instead of returning
			// a dangling UID.
			Eventually(func() bool {
				_, ok := m.GetUidByIdentifier(identifier)
				return ok
			}, time.Second*3, time.Millisecond*100).Should(BeFalse())
		})
	})

	// IsProcessAlive + EvictProcess back the "stale recovery" defense in
	// ApplyHttpChaos: when the host pod is deleted out from under us the
	// death-channel cleanup may not fire promptly, leaving a stale BPM entry
	// whose pipe FDs would block forever on write. Callers use
	// IsProcessAlive to decide whether to trust a recovered uid, and
	// EvictProcess to drop a stale entry without sending SIGTERM (since the
	// process is already gone).
	Context("liveness check and eviction", func() {
		It("IsProcessAlive returns true while the process runs and false after it exits", func() {
			cmd := DefaultProcessBuilder("sleep", "0.2").Build(context.Background())
			p, err := m.StartProcess(context.Background(), cmd)
			Expect(err).To(BeNil())

			// While alive: must report alive.
			Expect(m.IsProcessAlive(p.Uid)).To(BeTrue())

			// After exit: deathChannel will eventually remove the entry, at
			// which point IsProcessAlive returns false because the uid is
			// no longer tracked.
			Eventually(func() bool {
				return m.IsProcessAlive(p.Uid)
			}, time.Second*3, time.Millisecond*50).Should(BeFalse())
		})

		It("IsProcessAlive returns false after the host process is killed bypassing BPM", func() {
			// Spawn a sleep we can SIGKILL without going through BPM, to
			// simulate the host pod's namespace teardown reaping tproxy
			// before the deathChannel goroutine drains.
			cmd := DefaultProcessBuilder("sleep", "30").Build(context.Background())
			p, err := m.StartProcess(context.Background(), cmd)
			Expect(err).To(BeNil())

			Expect(m.IsProcessAlive(p.Uid)).To(BeTrue())

			// Reach around BPM and SIGKILL the process directly.
			Expect(p.Cmd.Process.Kill()).To(Succeed())

			// IsProcessAlive must report false even though BPM may not have
			// observed the death yet (the deathChannel goroutine drains
			// asynchronously). This is exactly the ApplyHttpChaos
			// stale-recovery scenario.
			Eventually(func() bool {
				return m.IsProcessAlive(p.Uid)
			}, time.Second*3, time.Millisecond*50).Should(BeFalse())
		})

		It("IsProcessAlive returns false for an unknown uid", func() {
			Expect(m.IsProcessAlive("never-registered-uid")).To(BeFalse())
		})

		It("EvictProcess unregisters the uid + identifier so a fresh start can re-register", func() {
			identifier := RandomeIdentifier()
			cmd := DefaultProcessBuilder("sleep", "30").
				SetIdentifier(identifier).
				Build(context.Background())
			p, err := m.StartProcess(context.Background(), cmd)
			Expect(err).To(BeNil())

			// Sanity: identifier is in use, so a second start must fail.
			cmd2 := DefaultProcessBuilder("sleep", "1").
				SetIdentifier(identifier).
				Build(context.Background())
			_, err = m.StartProcess(context.Background(), cmd2)
			Expect(err).NotTo(BeNil())
			Expect(strings.Contains(err.Error(), "is running")).To(BeTrue())

			// Evict the original (still actually running, but pretend it's
			// stale). All three internal maps must be cleared.
			m.EvictProcess(p.Uid)

			_, ok := m.GetUidByIdentifier(identifier)
			Expect(ok).To(BeFalse())
			_, ok = m.GetUID(p.Pair)
			Expect(ok).To(BeFalse())
			_, ok = m.GetPipes(p.Uid)
			Expect(ok).To(BeFalse())

			// And a fresh start on the same identifier now succeeds.
			cmd3 := DefaultProcessBuilder("sleep", "0.2").
				SetIdentifier(identifier).
				Build(context.Background())
			p3, err := m.StartProcess(context.Background(), cmd3)
			Expect(err).To(BeNil())

			// Cleanup: kill the still-running first process (we evicted its
			// BPM record but the OS process is alive).
			Expect(p.Cmd.Process.Kill()).To(Succeed())
			WaitProcess(m, p3, time.Second*3)
		})

		It("EvictProcess on an unknown uid is a no-op", func() {
			// Must not panic.
			m.EvictProcess("never-registered-uid")
		})
	})
})
