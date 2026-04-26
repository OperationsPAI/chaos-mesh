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
})
