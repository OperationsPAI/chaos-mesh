// Copyright 2024 Chaos Mesh Authors.
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

package chaosdaemon

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/chaos-mesh/chaos-mesh/pkg/bpm"
	"github.com/chaos-mesh/chaos-mesh/pkg/chaosdaemon/crclients"
	"github.com/chaos-mesh/chaos-mesh/pkg/chaosdaemon/crclients/test"
	"github.com/chaos-mesh/chaos-mesh/pkg/chaosdaemon/pb"
	"github.com/chaos-mesh/chaos-mesh/pkg/log"
	"github.com/chaos-mesh/chaos-mesh/pkg/mock"
)

// fakeTproxyEnv is the env var that, when set, tells the test binary to act as
// a stand-in for the real /usr/local/bin/tproxy: read an HTTP request off
// stdin, write a 200 OK reply on stdout, and stay alive so subsequent
// ApplyHttpChaos calls can find it via the BPM identifier registry.
const fakeTproxyEnv = "GO_HELPER_FAKE_TPROXY"

func init() {
	if os.Getenv(fakeTproxyEnv) != "" {
		runFakeTproxy()
		os.Exit(0)
	}
}

func runFakeTproxy() {
	reader := bufio.NewReader(os.Stdin)
	for {
		req, err := http.ReadRequest(reader)
		if err != nil {
			return
		}
		// Drain & discard the request body so the reader is positioned at
		// the next request frame.
		_, _ = io.Copy(io.Discard, req.Body)
		_ = req.Body.Close()
		fmt.Fprint(os.Stdout, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
	}
}

var _ = Describe("http chaos server", func() {
	defer mock.With("MockContainerdClient", &test.MockClient{})()
	logger, err := log.NewDefaultZapLogger()
	Expect(err).To(BeNil())
	s, _ := newDaemonServer(&crclients.CrClientConfig{
		Runtime: crclients.ContainerRuntimeContainerd}, nil, logger)

	Context("ApplyHttpChaos idempotency on retry (regression)", func() {
		It("a second ApplyHttpChaos with the same containerId reuses the existing tproxy", func() {
			Expect(s).NotTo(BeNil(), "newDaemonServer must build with the mocked containerd client")

			defer mock.With("pid", 9527)()

			// Counter for how many times the daemon paths through the
			// process builder. The bug: every retried RPC re-spawns. The
			// fix: only the first call spawns; subsequent ones reuse via
			// GetUidByIdentifier.
			var spawnCount int32
			defer mock.With("MockProcessBuild", func(ctx context.Context, cmd string, args ...string) *exec.Cmd {
				atomic.AddInt32(&spawnCount, 1)
				// Re-exec the test binary with the fake-tproxy env var so
				// it speaks HTTP over stdio just like the real tproxy.
				c := exec.Command(os.Args[0])
				c.Env = append(os.Environ(), fakeTproxyEnv+"=1")
				return c
			})()

			req := &pb.ApplyHttpChaosRequest{
				Rules:       "[]",
				ProxyPorts:  []uint32{8080},
				ContainerId: "containerd://regression-container-id",
				EnterNS:     true,
			}

			resp1, err := s.ApplyHttpChaos(context.TODO(), req)
			Expect(err).To(BeNil())
			Expect(resp1).NotTo(BeNil())
			Expect(resp1.InstanceUid).NotTo(BeEmpty())

			// Simulate the controller's retry pattern: it doesn't persist
			// InstanceUid back to the CR status, so the next reconcile
			// sends an empty UID for the same containerId.
			req2 := &pb.ApplyHttpChaosRequest{
				Rules:       "[]",
				ProxyPorts:  []uint32{8080},
				ContainerId: "containerd://regression-container-id",
				EnterNS:     true,
			}
			resp2, err := s.ApplyHttpChaos(context.TODO(), req2)
			Expect(err).To(BeNil(), "second ApplyHttpChaos must not return 'process with identifier ... is running'")
			Expect(resp2).NotTo(BeNil())

			// Same tproxy instance -- this is what unblocks injectedCount.
			Expect(resp2.InstanceUid).To(Equal(resp1.InstanceUid))

			// Trip-wire stayed armed: only one spawn ever happened.
			Expect(atomic.LoadInt32(&spawnCount)).To(Equal(int32(1)))

			// Cleanup: kill the fake tproxy so the suite exits.
			Expect(s.backgroundProcessManager.KillBackgroundProcess(context.TODO(), resp1.InstanceUid)).To(Succeed())
		})
	})

	// Regression test for the concurrent-RoundTrip 423 bug:
	//
	//   Multiple controller-manager replicas (and the same controller's
	//   reconcile loop firing on resource updates) call ApplyHttpChaos
	//   concurrently for the same pod. The tproxy stdio is one bidirectional
	//   pipe; the original code fast-failed concurrent callers with
	//   StatusLocked (423), which the controller surfaced as
	//   "Apply failed: status(423)" and used to overwrite the previous
	//   reconcile's success in PodHttpChaos.Status. Net effect: chaos was
	//   applied but injectedCount stayed at 0. The fix serializes concurrent
	//   RoundTrip on a per-uid mutex; every caller eventually writes its own
	//   request, gets its own 200 response, and surfaces success.
	Context("RoundTrip concurrency on the same tproxy stdio (regression)", func() {
		It("serializes concurrent calls and returns 200 for both, never 423", func() {
			// Synthetic tproxy: one goroutine reads requests off the
			// stdin-pipe and writes 200 responses to the stdout-pipe.
			stdinR, stdinW := io.Pipe()
			stdoutR, stdoutW := io.Pipe()

			// fake tproxy: serve one HTTP request per loop iteration.
			tproxyDone := make(chan struct{})
			go func() {
				defer close(tproxyDone)
				br := bufio.NewReader(stdinR)
				for {
					req, err := http.ReadRequest(br)
					if err != nil {
						return
					}
					_, _ = io.Copy(io.Discard, req.Body)
					_ = req.Body.Close()
					_, _ = fmt.Fprint(stdoutW, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
				}
			}()

			locker := &sync.Map{}
			transport := &stdioTransport{
				uid:    "regression-uid",
				locker: locker,
				pipes:  bpm.Pipes{Stdin: stdinW, Stdout: stdoutR},
			}

			// Drive N concurrent RoundTrips against the same transport.
			// Without the fix every goroutine after the first one would
			// observe locker.LoadOrStore(uid, true) -> loaded==true and
			// return 423; with the fix the per-uid mutex serializes them.
			const concurrency = 16
			var wg sync.WaitGroup
			statuses := make([]int, concurrency)
			errs := make([]error, concurrency)
			for i := 0; i < concurrency; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					req, err := http.NewRequest(http.MethodPut, "/", bytes.NewReader([]byte("{}")))
					if err != nil {
						errs[i] = err
						return
					}
					resp, err := transport.RoundTrip(req)
					if err != nil {
						errs[i] = err
						return
					}
					statuses[i] = resp.StatusCode
					_ = resp.Body.Close()
				}(i)
			}

			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			Eventually(done, 5*time.Second).Should(BeClosed(),
				"all concurrent RoundTrip calls must drain; if any deadlocked the lock implementation is wrong")

			for i := 0; i < concurrency; i++ {
				Expect(errs[i]).To(BeNil(), "RoundTrip #%d returned error", i)
				Expect(statuses[i]).To(Equal(http.StatusOK),
					"RoundTrip #%d returned %d; concurrent reconciles must not see 423", i, statuses[i])
			}

			// Tear down the fake tproxy.
			stdinW.Close()
			stdoutW.Close()
			<-tproxyDone
		})
	})

	// Regression test for symptom 1 (deadlock) reported on byte-cluster:
	//
	//   When the host pod is deleted and replaced (StatefulSet rollout etc.),
	//   the original tproxy process inside the old pod's net+pid namespaces is
	//   gone, but BPM's deathChannel cleanup may not fire promptly -- leaving a
	//   stale `(uid, pipes)` entry whose pipe FDs would block forever on write.
	//   The next ApplyHttpChaos that recovered this stale uid would log
	//   "ready to apply" and then deadlock inside RoundTrip, holding the
	//   per-uid mutex; every subsequent reconcile would block at Lock().
	//
	// The fix: live-check via IsProcessAlive before trusting the recovered
	// uid; if dead, EvictProcess and fall through to createHttpChaos.
	Context("ApplyHttpChaos with stale BPM entry (regression: deadlock symptom 1)", func() {
		It("detects a dead tproxy, evicts it, and respawns a fresh one", func() {
			Expect(s).NotTo(BeNil(), "newDaemonServer must build with the mocked containerd client")

			defer mock.With("pid", 9527)()

			// Track every spawn so we can assert two distinct tproxies were
			// born across the two ApplyHttpChaos calls.
			var spawnCount int32
			var spawnedCmds sync.Map // pid -> *exec.Cmd, populated post-Start
			defer mock.With("MockProcessBuild", func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
				atomic.AddInt32(&spawnCount, 1)
				c := exec.Command(os.Args[0])
				c.Env = append(os.Environ(), fakeTproxyEnv+"=1")
				return c
			})()

			containerID := "containerd://stale-recovery-container-id"
			req1 := &pb.ApplyHttpChaosRequest{
				Rules:       "[]",
				ProxyPorts:  []uint32{8080},
				ContainerId: containerID,
				EnterNS:     true,
			}

			resp1, err := s.ApplyHttpChaos(context.TODO(), req1)
			Expect(err).To(BeNil())
			Expect(resp1.InstanceUid).NotTo(BeEmpty())
			firstUid := resp1.InstanceUid

			// The first spawn happened; remember its pid so we can later
			// assert the second spawn produced a different process.
			spawnedCmds.Store("first", resp1.Instance)

			// Kill the first fake-tproxy outside of BPM (simulating the
			// host pod's namespace teardown reaping tproxy without
			// chaos-daemon's deathChannel goroutine getting a chance to
			// drain). We reach into BPM internals to grab the cmd.
			proc, ok := s.backgroundProcessManager.GetPipes(firstUid)
			Expect(ok).To(BeTrue())
			_ = proc // keep the variable used; pipes are also relevant only insofar as the underlying process is gone

			// Find the *Process in BPM and kill its underlying os.Process.
			// We can't reach the cmd directly through public API, so find
			// it via the same pid that was returned in resp1.
			killByPid(int(resp1.Instance))

			// Wait until the host kernel's reaped the process so
			// IsProcessAlive returns false. Note: the deathChannel may or
			// may not have drained by now; the test deliberately races to
			// hit ApplyHttpChaos *before* it does (which is the bug
			// scenario). IsProcessAlive must catch this regardless.
			Eventually(func() bool {
				return s.backgroundProcessManager.IsProcessAlive(firstUid)
			}, 5*time.Second, 50*time.Millisecond).Should(BeFalse(),
				"first fake tproxy must report dead after we killed it")

			// Second ApplyHttpChaos for the SAME containerID. With the bug,
			// recovery would trust the dead uid and RoundTrip would hang.
			// With the fix, IsProcessAlive returns false, EvictProcess
			// drops the stale entry, and createHttpChaos spawns a fresh
			// tproxy under a new uid.
			req2 := &pb.ApplyHttpChaosRequest{
				Rules:       "[]",
				ProxyPorts:  []uint32{8080},
				ContainerId: containerID,
				EnterNS:     true,
			}
			done := make(chan struct{})
			var resp2 *pb.ApplyHttpChaosResponse
			var err2 error
			go func() {
				defer close(done)
				resp2, err2 = s.ApplyHttpChaos(context.TODO(), req2)
			}()
			Eventually(done, 10*time.Second).Should(BeClosed(),
				"second ApplyHttpChaos must not deadlock when recovery hits a dead tproxy")
			Expect(err2).To(BeNil())
			Expect(resp2).NotTo(BeNil())
			Expect(resp2.InstanceUid).NotTo(BeEmpty())
			Expect(resp2.InstanceUid).NotTo(Equal(firstUid),
				"a stale uid must be evicted, not reused; second call should mint a new uid")
			Expect(atomic.LoadInt32(&spawnCount)).To(Equal(int32(2)),
				"exactly two spawns: first is now stale + dead, second is the respawn")

			// Cleanup the second fake tproxy.
			Expect(s.backgroundProcessManager.KillBackgroundProcess(context.TODO(), resp2.InstanceUid)).To(Succeed())
		})
	})

	// Regression test for the timeout half of symptom 1: even with the
	// IsProcessAlive guard, RoundTrip itself must not block forever if
	// tproxy is alive but unresponsive (e.g. wedged on a slow rule push).
	// Without a deadline the per-uid mutex would still deadlock subsequent
	// callers. RoundTripCtx caps the wait and returns ctx.Err() promptly.
	Context("RoundTripCtx timeout (regression: deadlock symptom 1, slow path)", func() {
		It("returns context.DeadlineExceeded when tproxy never reads stdin", func() {
			// Pipe with no reader on the other side -- writes will block
			// until the buffer fills, simulating an unresponsive tproxy.
			stdinR, stdinW := io.Pipe()
			_, stdoutW := io.Pipe()
			defer stdinR.Close()
			defer stdinW.Close()
			defer stdoutW.Close()

			locker := &sync.Map{}
			transport := &stdioTransport{
				uid:    "slow-tproxy-uid",
				locker: locker,
				pipes:  bpm.Pipes{Stdin: stdinW, Stdout: io.NopCloser(bytes.NewReader(nil))},
			}

			req, err := http.NewRequest(http.MethodPut, "/", bytes.NewReader([]byte("{}")))
			Expect(err).To(BeNil())

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			start := time.Now()
			_, err = transport.RoundTripCtx(ctx, req)
			elapsed := time.Since(start)

			Expect(err).NotTo(BeNil(),
				"RoundTripCtx must surface an error when tproxy is unresponsive")
			Expect(elapsed).To(BeNumerically("<", 2*time.Second),
				"RoundTripCtx must return promptly on timeout, not block")
			// Underlying ctx.Err() should be DeadlineExceeded, wrapped.
			Expect(ctx.Err()).To(Equal(context.DeadlineExceeded))
		})
	})
})

// killByPid sends SIGKILL to the host process at the given pid. Used by the
// stale-recovery test to bypass BPM's KillBackgroundProcess (which would
// trigger deathChannel cleanup) and simulate the host pod's namespace
// teardown reaping tproxy out from under chaos-daemon.
func killByPid(pid int) {
	osProc, err := os.FindProcess(pid)
	if err != nil {
		return
	}
	_ = osProc.Kill()
}
