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

package chaosdaemon

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/chaos-mesh/chaos-mesh/pkg/bpm"
	pb "github.com/chaos-mesh/chaos-mesh/pkg/chaosdaemon/pb"
	"github.com/chaos-mesh/chaos-mesh/pkg/chaosdaemon/tproxyconfig"
)

const (
	tproxyBin = "/usr/local/bin/tproxy"
	pathEnv   = "PATH"

	// httpChaosRoundTripTimeout caps a single tproxy stdio request/response
	// cycle. If the underlying tproxy process is dead or unresponsive, the
	// stdin write or stdout read can block indefinitely; without a cap the
	// per-uid mutex in stdioTransport.RoundTrip would never release and every
	// subsequent reconcile would deadlock at Lock(). The cap is intentionally
	// shorter than typical gRPC-client deadlines so the caller can return a
	// clean error and let the controller retry / evict.
	httpChaosRoundTripTimeout = 5 * time.Second
)

type stdioTransport struct {
	uid    string
	locker *sync.Map
	pipes  bpm.Pipes
}

// stdioMu returns the per-uid mutex that serializes RoundTrip writes/reads on
// the same tproxy stdio. The mutex is created on first access and shared via
// the daemon-scoped sync.Map (DaemonServer.tproxyLocker), so all
// stdioTransport instances built for the same uid block on the same lock.
func (t *stdioTransport) stdioMu() *sync.Mutex {
	mu, _ := t.locker.LoadOrStore(t.uid, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

// RoundTripCtx is the context-aware entry point. It runs RoundTrip in a
// goroutine, then either returns its result or, if the context fires first,
// returns a deadline-exceeded error.
//
// Why this matters: if the underlying tproxy process has died but BPM still
// has its pipe FDs (e.g. the host pod was deleted out from under us), a
// stdin write or stdout read can block forever. Without a deadline the
// per-uid mutex held by RoundTrip would never release, and every subsequent
// reconcile would deadlock at Lock() and then time out at the gRPC layer
// with "context canceled" events on the CR.
//
// Note: the spawned goroutine still holds the mutex if its I/O hangs. The
// caller (ApplyHttpChaos) is responsible for evicting the BPM entry on
// timeout so subsequent calls re-spawn a fresh tproxy under a fresh uid
// (and thus a different mutex), instead of queueing behind the dead one.
func (t *stdioTransport) RoundTripCtx(ctx context.Context, req *http.Request) (*http.Response, error) {
	type result struct {
		resp *http.Response
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := t.RoundTrip(req)
		done <- result{resp: resp, err: err}
	}()
	select {
	case r := <-done:
		return r.resp, r.err
	case <-ctx.Done():
		return nil, errors.Wrapf(ctx.Err(), "tproxy stdio roundtrip aborted (uid=%s)", t.uid)
	}
}

func (t *stdioTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	// Serialize concurrent reconciles for the same tproxy.
	//
	// Multiple controller-manager replicas (and the same controller's own
	// reconcile loop firing on resource updates) can call ApplyHttpChaos
	// concurrently for the same pod. The tproxy stdio is a single bidirectional
	// pipe -- two callers writing requests at the same time would interleave
	// bytes mid-frame and corrupt both responses. The original implementation
	// fast-failed concurrent callers with 423 Locked; the controller then
	// surfaced that as "Apply failed" and overwrote the previous reconcile's
	// success in PodHttpChaos.Status, leaving injectedCount stuck at 0 even
	// though the chaos was actually applied. Block on a per-uid mutex instead;
	// every caller writes its own request, reads its own response, and returns
	// 200. The mutex is allocated on first access via LoadOrStore so concurrent
	// allocations don't race.
	mu := t.stdioMu()
	mu.Lock()
	defer mu.Unlock()

	if t.pipes.Stdin == nil {
		return nil, errors.New("fail to get stdin of process")
	}
	if t.pipes.Stdout == nil {
		return nil, errors.New("fail to get stdout of process")
	}

	err = req.Write(t.pipes.Stdin)
	if err != nil {
		return
	}

	resp, err = http.ReadResponse(bufio.NewReader(t.pipes.Stdout), req)
	return
}

// httpChaosIdentifier returns the BPM identifier under which the tproxy
// process for a given container is registered. Keeping it as a single helper
// lets ApplyHttpChaos and createHttpChaos agree on the exact key to look up.
func httpChaosIdentifier(containerID string) string {
	return fmt.Sprintf("tproxy-%s", containerID)
}

func (s *DaemonServer) ApplyHttpChaos(ctx context.Context, in *pb.ApplyHttpChaosRequest) (*pb.ApplyHttpChaosResponse, error) {
	log := s.getLoggerFromContext(ctx)
	log.Info("applying http chaos")

	if in.InstanceUid == "" {
		if uid, ok := s.backgroundProcessManager.GetUID(bpm.ProcessPair{Pid: int(in.Instance), CreateTime: in.StartTime}); ok {
			in.InstanceUid = uid
		}
	}

	// Idempotency: if the controller / RPC retries (which it routinely does
	// while the PodHttpChaos status update lands), the InstanceUid the caller
	// sends is empty and the (Pid, StartTime) reverse lookup misses for any of
	// the usual reasons (zero-valued status fields, daemon-side restart, race
	// with the controller's deferred status update). Without recovery, the next
	// createHttpChaos call would fail with "process with identifier
	// tproxy-<containerId> is running" and HTTPChaos would be stuck at
	// injectedCount=0 forever even though tproxy is healthy inside the pod.
	// Look up the live tproxy process directly by its identifier and reuse it.
	if in.InstanceUid == "" && in.ContainerId != "" {
		if uid, ok := s.backgroundProcessManager.GetUidByIdentifier(httpChaosIdentifier(in.ContainerId)); ok {
			// Live-check before trusting the recovered uid. If the host pod
			// was deleted and replaced (StatefulSet rollout, eviction, etc.)
			// the original tproxy is gone but the deathChannel cleanup may
			// not have fired yet (e.g. cmd.Wait stuck across the namespace
			// teardown, or the controller hit us before the chan drained).
			// Trusting a stale uid sends RoundTrip into a dead pipe that
			// blocks forever, which then deadlocks every subsequent
			// reconcile via the per-uid mutex.
			if s.backgroundProcessManager.IsProcessAlive(uid) {
				log.Info("recovered existing tproxy", "uid", uid, "containerId", in.ContainerId)
				in.InstanceUid = uid
			} else {
				log.Info("stale tproxy entry, evicting and respawning",
					"uid", uid, "containerId", in.ContainerId)
				s.backgroundProcessManager.EvictProcess(uid)
				// fall through to createHttpChaos below
			}
		}
	}

	if _, ok := s.backgroundProcessManager.GetPipes(in.InstanceUid); !ok {
		if in.InstanceUid != "" {
			// chaos daemon may restart, create another tproxy instance
			if err := s.backgroundProcessManager.KillBackgroundProcess(ctx, in.InstanceUid); err != nil {
				// ignore this error
				log.Error(err, "kill background process", "uid", in.InstanceUid)
			}
		}

		// set uid internally
		if err := s.createHttpChaos(ctx, in); err != nil {
			return nil, errors.Wrap(err, "create http chaos")
		}
	}

	resp, err := s.applyHttpChaos(ctx, in)
	if err != nil {
		if killError := s.backgroundProcessManager.KillBackgroundProcess(ctx, in.InstanceUid); killError != nil {
			log.Error(killError, "kill tproxy", "uid", in.InstanceUid)
		}
		return nil, errors.Wrap(err, "apply config")
	}
	return resp, err
}

func (s *DaemonServer) applyHttpChaos(ctx context.Context, in *pb.ApplyHttpChaosRequest) (*pb.ApplyHttpChaosResponse, error) {
	log := s.getLoggerFromContext(ctx)

	pipes, ok := s.backgroundProcessManager.GetPipes(in.InstanceUid)
	if !ok {
		return nil, errors.Errorf("fail to get process(%s)", in.InstanceUid)
	}

	transport := &stdioTransport{
		uid:    in.InstanceUid,
		locker: s.tproxyLocker,
		pipes:  pipes,
	}

	var rules []tproxyconfig.PodHttpChaosBaseRule
	err := json.Unmarshal([]byte(in.Rules), &rules)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal rules")
	}

	log.Info("the length of actions", "length", len(rules))

	httpChaosSpec := tproxyconfig.Config{
		ProxyPorts: in.ProxyPorts,
		Rules:      rules,
	}

	if len(in.Tls) != 0 {
		httpChaosSpec.TLS = new(tproxyconfig.TLSConfig)
		err = json.Unmarshal([]byte(in.Tls), httpChaosSpec.TLS)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshal tls config")
		}
	}

	config, err := json.Marshal(&httpChaosSpec)
	if err != nil {
		return nil, err
	}

	log.Info("ready to apply", "config", string(config))

	req, err := http.NewRequest(http.MethodPut, "/", bytes.NewReader(config))
	if err != nil {
		return nil, errors.Wrap(err, "create http request")
	}

	// Cap the stdio roundtrip with our own deadline. Use the more restrictive
	// of the caller's gRPC ctx and our internal cap: never block longer than
	// httpChaosRoundTripTimeout, but exit early if the gRPC client gives up
	// first. On timeout we evict the BPM entry so the next reconcile starts
	// fresh -- otherwise the in-flight goroutine pins the per-uid mutex
	// behind a dead pipe and every subsequent caller deadlocks too.
	rtCtx, cancel := context.WithTimeout(ctx, httpChaosRoundTripTimeout)
	defer cancel()

	resp, err := transport.RoundTripCtx(rtCtx, req)
	if err != nil {
		if rtCtx.Err() != nil {
			log.Info("tproxy unresponsive, evicting BPM entry so next reconcile respawns",
				"uid", in.InstanceUid, "timeout", httpChaosRoundTripTimeout)
			s.backgroundProcessManager.EvictProcess(in.InstanceUid)
		}
		return nil, errors.Wrap(err, "send http request")
	}

	log.Info("http chaos applied")

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	return &pb.ApplyHttpChaosResponse{
		Instance:    int64(in.Instance),
		InstanceUid: in.InstanceUid,
		StartTime:   in.StartTime,
		StatusCode:  int32(resp.StatusCode),
		Error:       string(body),
	}, nil
}

func (s *DaemonServer) createHttpChaos(ctx context.Context, in *pb.ApplyHttpChaosRequest) error {
	pid, err := s.crClient.GetPidFromContainerID(ctx, in.ContainerId)
	if err != nil {
		return errors.Wrapf(err, "get PID of container(%s)", in.ContainerId)
	}
	args := []string{"-i", "-vv"}
	if s.TproxyBridgeless {
		args = append(args, "--bridgeless")
	}
	processBuilder := bpm.DefaultProcessBuilder(tproxyBin, args...).
		EnableLocalMnt().
		SetIdentifier(httpChaosIdentifier(in.ContainerId)).
		SetEnv(pathEnv, os.Getenv(pathEnv))

	if in.EnterNS {
		processBuilder = processBuilder.SetNS(pid, bpm.PidNS).SetNS(pid, bpm.NetNS)
	}

	cmd := processBuilder.Build(ctx)
	cmd.Stderr = os.Stderr

	proc, err := s.backgroundProcessManager.StartProcess(ctx, cmd)
	if err != nil {
		return errors.Wrapf(err, "execute command(%s)", cmd)
	}

	in.Instance = int64(proc.Pair.Pid)
	in.StartTime = proc.Pair.CreateTime
	in.InstanceUid = proc.Uid
	return nil
}
