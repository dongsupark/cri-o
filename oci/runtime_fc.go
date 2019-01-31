package oci

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	_ "path/filepath"
	"syscall"
	"time"
	"unsafe"

	firecracker "github.com/firecracker-microvm/firecracker-go-sdk"
	fclient "github.com/firecracker-microvm/firecracker-go-sdk/client"
	fcmodels "github.com/firecracker-microvm/firecracker-go-sdk/client/models"
	fcops "github.com/firecracker-microvm/firecracker-go-sdk/client/operations"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	rspec "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"k8s.io/client-go/tools/remotecommand"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	utilexec "k8s.io/utils/exec"
)

const (
	fcTimeout         = 10
	fifoCrioDir       = "/tmp/crio/fifo"
	defaultConfigPath = "/etc/crio/firecracker-crio.json"

	defaultVsockPort     = 10789
	supportedMountFSType = "ext4"
	rootDevice           = "root=/dev/vda1"

	defaultCID  uint32 = 3
	defaultPort uint32 = 655
)

type vmmState uint8

const (
	notReady vmmState = iota
	apiReady
	vmReady
)

type FcConfig struct {
	FirecrackerBinaryPath string `json:"firecracker_binary_path"`
	SocketPath            string `json:"socket_path"`
	KernelImagePath       string `json:"kernel_image_path"`
	KernelArgs            string `json:"kernel_args"`
	RootDrive             string `json:"root_drive"`
}

func LoadConfig(path string) (*FcConfig, error) {
	if path == "" {
		path = defaultConfigPath
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg FcConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// RuntimeFC is the Runtime interface implementation that is more appropriate
// for FC based container runtimes.
type RuntimeFC struct {
	RuntimeBase

	ctx context.Context

	firecrackerd *exec.Cmd            // firecrackerd process
	fcClient     *fclient.Firecracker // the current active connection

	machine    *firecracker.Machine
	machineCID uint32
	config     *FcConfig
}

// NewRuntimeFC creates a new RuntimeFC instance
func NewRuntimeFC(rb RuntimeBase) (RuntimeImpl, error) {
	config, err := LoadConfig(defaultConfigPath)
	if err != nil {
		return nil, err
	}

	return &RuntimeFC{
		RuntimeBase: rb,
		ctx:         context.Background(),
		config:      config,
	}, nil
}

// Version returns the version of the OCI Runtime
func (r *RuntimeFC) Version() (string, error) {
	return "vm", nil
}

func (r *RuntimeFC) newFireClient() *fclient.Firecracker {
	httpClient := fclient.NewHTTPClient(strfmt.NewFormats())

	socketTransport := &http.Transport{
		DialContext: func(ctx context.Context, network, path string) (net.Conn, error) {
			addr, err := net.ResolveUnixAddr("unix", r.config.SocketPath)
			if err != nil {
				return nil, err
			}

			return net.DialUnix("unix", nil, addr)
		},
	}

	transport := httptransport.New(fclient.DefaultHost, fclient.DefaultBasePath, fclient.DefaultSchemes)
	transport.Transport = socketTransport
	httpClient.SetTransport(transport)

	return httpClient
}

func (r *RuntimeFC) vmRunning() bool {
	resp, err := r.client().Operations.DescribeInstance(nil)
	if err != nil {
		return false
	}

	// Be explicit
	switch resp.Payload.State {
	case fcmodels.InstanceInfoStateStarting:
		// Unsure what we should do here
		logrus.Errorf("unexpected-state %v", fcmodels.InstanceInfoStateStarting)
		return false
	case fcmodels.InstanceInfoStateRunning:
		return true
	case fcmodels.InstanceInfoStateUninitialized, fcmodels.InstanceInfoStateHalting, fcmodels.InstanceInfoStateHalted:
		return false
	default:
		return false
	}
}

// waitVMM waits for the VMM to be up and running.
func (r *RuntimeFC) waitVMM(timeout int) error {
	if timeout < 0 {
		return fmt.Errorf("Invalid timeout %ds", timeout)
	}

	timeStart := time.Now()
	for {
		_, err := r.client().Operations.DescribeInstance(nil)
		if err == nil {
			return nil
		}

		if int(time.Now().Sub(timeStart).Seconds()) > timeout {
			return fmt.Errorf("Failed to connect to firecrackerinstance (timeout %ds): %v", timeout, err)
		}

		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

func (r *RuntimeFC) fcInit(timeout int) error {
	_ = r.fcCleanup()

	args := []string{"--api-sock", r.config.SocketPath}

	fmt.Printf("running %v %v %v\n", r.config.FirecrackerBinaryPath, args[0], args[1])

	cmd := exec.Command(r.config.FirecrackerBinaryPath, args...)
	if err := cmd.Start(); err != nil {
		logrus.Errorf("Error starting firecracker", err)
		return err
	}

	//     r.info.PID = cmd.Process.Pid
	r.firecrackerd = cmd
	r.fcClient = r.newFireClient()

	//     if err := r.waitVMM(timeout); err != nil {
	//         logrus.Errorf("fcInit failed:", err)
	//         return err
	//     }

	//     r.state.set(apiReady)

	// Store VMM information
	//     return r.storage.storeHypervisorState(fc.id, fc.info)

	return nil
}

func (r *RuntimeFC) client() *fclient.Firecracker {
	if r.fcClient == nil {
		r.fcClient = r.newFireClient()
	}

	return r.fcClient
}

func (r *RuntimeFC) fcSetBootSource(path, params string) error {
	bootParams := params + " " + rootDevice
	bootSrcParams := fcops.NewPutGuestBootSourceParams()
	src := &fcmodels.BootSource{
		KernelImagePath: &path,
		BootArgs:        bootParams,
	}
	bootSrcParams.SetBody(src)

	_, err := r.client().Operations.PutGuestBootSource(bootSrcParams)
	if err != nil {
		return err
	}

	return nil
}

func (r *RuntimeFC) fcSetVMRootfs(path string) error {
	driveID := "rootfs"
	driveParams := fcops.NewPutGuestDriveByIDParams()
	driveParams.SetDriveID(driveID)
	isReadOnly := false

	//Add it as a regular block device
	//This allows us to use a paritioned root block device
	isRootDevice := false
	drive := &fcmodels.Drive{
		DriveID:      &driveID,
		IsReadOnly:   &isReadOnly,
		IsRootDevice: &isRootDevice,
		PathOnHost:   &path,
	}
	driveParams.SetBody(drive)
	_, err := r.client().Operations.PutGuestDriveByID(driveParams)
	if err != nil {
		return err
	}

	return nil
}

// CreateContainer creates a container.
func (r *RuntimeFC) CreateContainer(c *Container, cgroupParent string) (err error) {
	// Lock the container
	c.opLock.Lock()
	defer c.opLock.Unlock()

	// First thing, we need to start the runtime daemon
	if err = r.startVM(c); err != nil {
		return err
	}

	return nil
}

func (r *RuntimeFC) startVM(c *Container) error {
	_ = r.fcCleanup()

	r.fcClient = r.newFireClient()

	// temporarily disable vsock, because of "exit 148" issue.

	//     cid, err := findNextAvailableVsockCID(r.ctx)
	//     if err != nil {
	//         return err
	//     }

	cfg := firecracker.Config{
		SocketPath: r.config.SocketPath,
		//         VsockDevices:    []firecracker.VsockDevice{{Path: "root", CID: cid}},
		KernelImagePath: r.config.KernelImagePath,
		KernelArgs:      r.config.KernelArgs,
		MachineCfg: fcmodels.MachineConfiguration{
			MemSizeMib: 128,
		},
		Debug:             true,
		DisableValidation: true,
	}

	driveBuilder := firecracker.NewDrivesBuilder(r.config.RootDrive)

	cfg.Drives = driveBuilder.Build()

	cmdBuilder := firecracker.VMCommandBuilder{}.
		WithBin(r.config.FirecrackerBinaryPath).
		WithSocketPath(r.config.SocketPath).
		Build(r.ctx)

	vmmCtx, vmmCancel := context.WithCancel(context.Background())
	defer vmmCancel()

	var errMach error
	r.machine, errMach = firecracker.NewMachine(vmmCtx, cfg, firecracker.WithProcessRunner(cmdBuilder))
	if errMach != nil {
		return errMach
	}
	//     r.machineCID = cid

	r.fcSetBootSource(r.config.KernelImagePath, r.config.KernelArgs)
	r.fcSetVMRootfs(r.config.RootDrive)

	if err := r.machine.Start(vmmCtx); err != nil {
		return err
	}

	//     if !r.vmRunning() {
	//         if err := r.fcStartVM(); err != nil {
	//             return err
	//         }
	//     }

	return r.waitVMM(fcTimeout)
}

func (r *RuntimeFC) fcStartVM() error {
	r.fcClient = r.newFireClient()

	actionParams := fcops.NewCreateSyncActionParams()
	actionInfo := &fcmodels.InstanceActionInfo{
		ActionType: "InstanceStart",
	}
	actionParams.SetInfo(actionInfo)
	_, err := r.client().Operations.CreateSyncAction(actionParams)
	if err != nil {
		return err
	}

	return nil
}

func findNextAvailableVsockCID(ctx context.Context) (uint32, error) {
	const (
		// VHOST_VSOCK_SET_GUEST_CID in vhost.h
		ioctlVsockSetGuestCID = uintptr(0x4008AF60)

		startCID        = 3
		maxCID          = math.MaxUint32
		vsockDevicePath = "/dev/vhost-vsock"
	)

	file, err := os.OpenFile(vsockDevicePath, unix.O_RDWR, os.FileMode(0600))
	if err != nil {
		return 0, fmt.Errorf("cannot open vsock device")
	}
	defer file.Close()

	for contextID := startCID; contextID < maxCID; contextID++ {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			cid := contextID
			_, _, err = syscall.Syscall(
				unix.SYS_IOCTL,
				file.Fd(),
				ioctlVsockSetGuestCID,
				uintptr(unsafe.Pointer(&cid)))

			switch err {
			case syscall.Errno(0):
				return uint32(contextID), nil
			case syscall.EADDRINUSE:
				continue
			default:
				return 0, err
			}
		}
	}

	return 0, fmt.Errorf("cannot find a vsock context id")
}

// StartContainer starts a container.
func (r *RuntimeFC) StartContainer(c *Container) error {
	// Lock the container
	c.opLock.Lock()
	defer c.opLock.Unlock()

	if err := r.start(r.ctx, c.ID(), ""); err != nil {
		return err
	}

	// Spawn a goroutine waiting for the container to terminate. Once it
	// happens, the container status is retrieved to be updated.
	go func() {
		r.wait(r.ctx, c.ID(), "")
		r.UpdateContainerStatus(c)
	}()

	return nil
}

// ExecContainer prepares a streaming endpoint to execute a command in the container.
func (r *RuntimeFC) ExecContainer(c *Container, cmd []string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	exitCode, err := r.execContainer(c, cmd, 0, stdin, stdout, stderr, tty, resize)
	if err != nil {
		return err
	}
	if exitCode != 0 {
		return &utilexec.CodeExitError{
			Err:  errors.Errorf("error executing command %v, exit code %d", cmd, exitCode),
			Code: int(exitCode),
		}
	}

	return nil
}

// ExecSyncContainer execs a command in a container and returns it's stdout, stderr and return code.
func (r *RuntimeFC) ExecSyncContainer(c *Container, command []string, timeout int64) (*ExecSyncResponse, error) {
	var stdoutBuf, stderrBuf bytes.Buffer
	//     stdout := cioutil.NewNopWriteCloser(&stdoutBuf)
	//     stderr := cioutil.NewNopWriteCloser(&stderrBuf)

	//     exitCode, err := r.execContainer(c, command, timeout, nil, stdout, stderr, c.terminal, nil)
	//     if err != nil {
	//         return nil, ExecSyncError{
	//             ExitCode: -1,
	//             Err:      errors.Wrapf(err, "ExecSyncContainer failed"),
	//         }
	//     }

	return &ExecSyncResponse{
		Stdout: stdoutBuf.Bytes(),
		Stderr: stderrBuf.Bytes(),
		//         ExitCode: exitCode,
	}, nil
}

func (r *RuntimeFC) execContainer(c *Container, cmd []string, timeout int64, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) (exitCode int32, err error) {

	// Cancel the context before returning to ensure goroutines are stopped.
	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()

	// Generate a unique execID
	execID := fcGenerateID()

	// Create IO fifos
	//     execIO, err := cio.NewExecIO(c.ID(), fifoGlobalDir, tty, stdin != nil)
	//     if err != nil {
	//         return -1, err
	//     }
	//     defer execIO.Close()

	//     execIO.Attach(cio.AttachOptions{
	//         Stdin:     stdin,
	//         Stdout:    stdout,
	//         Stderr:    stderr,
	//         Tty:       tty,
	//         StdinOnce: true,
	//         CloseStdin: func() error {
	//             return r.closeIO(ctx, c.ID(), execID)
	//         },
	//     })

	pSpec := c.Spec().Process
	pSpec.Args = cmd

	defer func() {
		if err != nil {
			r.remove(ctx, c.ID(), execID)
		}
	}()

	// Start the process
	if err = r.start(ctx, c.ID(), execID); err != nil {
		return -1, err
	}

	// Initialize terminal resizing if necessary
	if resize != nil {
		kubecontainer.HandleResizing(resize, func(size remotecommand.TerminalSize) {
			if err := r.resizePty(ctx, c.ID(), execID, size); err != nil {
				logrus.Warnf("Failed to resize terminal: %v", err)
			}
		})
	}

	timeoutDuration := time.Duration(timeout) * time.Second

	var timeoutCh <-chan time.Time
	if timeoutDuration == 0 {
		// Do not set timeout if it's 0
		timeoutCh = make(chan time.Time)
	} else {
		timeoutCh = time.After(timeoutDuration)
	}

	execCh := make(chan error)
	go func() {
		// Wait for the process to terminate
		exitCode, _, err = r.wait(ctx, c.ID(), execID)
		if err != nil {
			execCh <- err
		}

		close(execCh)
	}()

	select {
	case err = <-execCh:
		if err != nil {
			r.kill(ctx, c.ID(), execID, syscall.SIGKILL, false)
			return -1, err
		}
	case <-timeoutCh:
		r.kill(ctx, c.ID(), execID, syscall.SIGKILL, false)
		<-execCh
		return -1, errors.Errorf("ExecSyncContainer timeout (%v)", timeoutDuration)
	}

	// Delete the process
	if err := r.remove(ctx, c.ID(), execID); err != nil {
		return -1, err
	}

	return
}

// fcGenerateID generates a random unique id.
func fcGenerateID() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// UpdateContainer updates container resources
func (r *RuntimeFC) UpdateContainer(c *Container, res *rspec.LinuxResources) error {
	// Lock the container
	c.opLock.Lock()
	defer c.opLock.Unlock()

	return nil
}

// WaitContainerStateStopped runs a loop polling UpdateStatus(), seeking for
// the container status to be updated to 'stopped'. Either it gets the expected
// status and returns nil, or it reaches the timeout and returns an error.
func (r *RuntimeFC) WaitContainerStateStopped(ctx context.Context, c *Container) (err error) {
	return waitContainerStateStopped(ctx, c, r, r.RuntimeBase)
}

// StopContainer stops a container. Timeout is given in seconds.
func (r *RuntimeFC) StopContainer(ctx context.Context, c *Container, timeout int64) error {
	// Lock the container
	c.opLock.Lock()
	defer c.opLock.Unlock()

	return r.stopVM()

	/*
			// Cancel the context before returning to ensure goroutines are stopped.
			ctx, cancel := context.WithCancel(r.ctx)
			defer cancel()

			stopCh := make(chan error)
			go func() {
				if _, _, err := r.wait(ctx, c.ID(), ""); err != nil {
					stopCh <- err
				}

				close(stopCh)
			}()

			var sig syscall.Signal

			if timeout > 0 {
				sig = c.GetStopSignalInt()
				// Send a stopping signal to the container
				if err := r.kill(ctx, c.ID(), "", sig, false); err != nil {
					return err
				}

				timeoutDuration := time.Duration(timeout) * time.Second

				err := r.waitCtrTerminate(sig, stopCh, timeoutDuration)
				if err == nil {
					return nil
				}
				logrus.Warnf("%v", err)
			}

			sig = syscall.SIGKILL
			// Send a SIGKILL signal to the container
			if err := r.kill(ctx, c.ID(), "", sig, false); err != nil {
				return err
			}

			if err := r.waitCtrTerminate(sig, stopCh, killContainerTimeout); err != nil {
				logrus.Errorf("%v", err)
				return err
			}

		return nil
	*/
}

func (r *RuntimeFC) stopVM() error {
	return r.machine.StopVMM()
}

func (r *RuntimeFC) waitCtrTerminate(sig syscall.Signal, stopCh chan error, timeout time.Duration) error {
	select {
	case err := <-stopCh:
		return err
	case <-time.After(timeout):
		return errors.Errorf("StopContainer with signal %v timed out after (%v)", sig, timeout)
	}
}

// DeleteContainer deletes a container.
func (r *RuntimeFC) DeleteContainer(c *Container) error {
	// Lock the container
	c.opLock.Lock()
	defer c.opLock.Unlock()

	_ = r.fcCleanup()

	//     cInfo, ok := r.ctrs[c.ID()]
	//     if !ok {
	//         return errors.New("Could not retrieve container information")
	//     }

	//     if err := cInfo.cio.Close(); err != nil {
	//         return err
	//     }

	if err := r.remove(r.ctx, c.ID(), ""); err != nil {
		return err
	}

	//     delete(r.ctrs, c.ID())

	return nil
}

func (r *RuntimeFC) fcCleanup() error {
	logrus.Infof("Cleaning up firecracker socket %s", r.config.SocketPath)

	cmd := exec.Command("/bin/rm", "-f", r.config.SocketPath)
	if err := cmd.Start(); err != nil {
		logrus.Errorf("Error cleaning up firecracker", err)
		return err
	}
	return nil
}

// UpdateContainerStatus refreshes the status of the container.
func (r *RuntimeFC) UpdateContainerStatus(c *Container) error {
	// Lock the container
	c.opLock.Lock()
	defer c.opLock.Unlock()

	return nil
}

// PauseContainer pauses a container.
func (r *RuntimeFC) PauseContainer(c *Container) error {
	// Lock the container
	c.opLock.Lock()
	defer c.opLock.Unlock()

	return nil
}

// UnpauseContainer unpauses a container.
func (r *RuntimeFC) UnpauseContainer(c *Container) error {
	// Lock the container
	c.opLock.Lock()
	defer c.opLock.Unlock()

	return nil
}

// ContainerStats provides statistics of a container.
func (r *RuntimeFC) ContainerStats(c *Container) (*ContainerStats, error) {
	// Lock the container with a shared lock
	c.opLock.RLock()
	defer c.opLock.RUnlock()

	//     return metricsToCtrStats(c, metrics), nil
	return &ContainerStats{}, nil
}

// SignalContainer sends a signal to a container process.
func (r *RuntimeFC) SignalContainer(c *Container, sig syscall.Signal) error {
	// Lock the container
	c.opLock.Lock()
	defer c.opLock.Unlock()

	return r.kill(r.ctx, c.ID(), "", sig, true)
}

// AttachContainer attaches IO to a running container.
func (r *RuntimeFC) AttachContainer(c *Container, inputStream io.Reader, outputStream, errorStream io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	// Initialize terminal resizing
	kubecontainer.HandleResizing(resize, func(size remotecommand.TerminalSize) {
		if err := r.resizePty(r.ctx, c.ID(), "", size); err != nil {
			logrus.Warnf("Failed to resize terminal: %v", err)
		}
	})

	//     cInfo, ok := r.ctrs[c.ID()]
	//     if !ok {
	//         return errors.New("Could not retrieve container information")
	//     }

	//     opts := cio.AttachOptions{
	//         Stdin:     inputStream,
	//         Stdout:    outputStream,
	//         Stderr:    errorStream,
	//         Tty:       tty,
	//         StdinOnce: c.stdinOnce,
	//         CloseStdin: func() error {
	//             return r.closeIO(r.ctx, c.ID(), "")
	//         },
	//     }

	//     cInfo.cio.Attach(opts)

	return nil
}

// PortForwardContainer forwards the specified port provides statistics of a container.
func (r *RuntimeFC) PortForwardContainer(c *Container, port int32, stream io.ReadWriter) error {
	return nil
}

// ReopenContainerLog reopens the log file of a container.
func (r *RuntimeFC) ReopenContainerLog(c *Container) error {
	return nil
}

func (r *RuntimeFC) start(ctx context.Context, ctrID, execID string) error {
	return nil
}

func (r *RuntimeFC) wait(ctx context.Context, ctrID, execID string) (int32, time.Time, error) {
	//     return int32(resp.ExitStatus), resp.ExitedAt, nil
	return int32(1), time.Now(), nil
}

func (r *RuntimeFC) kill(ctx context.Context, ctrID, execID string, signal syscall.Signal, all bool) error {
	return nil
}

func (r *RuntimeFC) remove(ctx context.Context, ctrID, execID string) error {
	return nil
}

func (r RuntimeFC) resizePty(ctx context.Context, ctrID, execID string, size remotecommand.TerminalSize) error {
	return nil
}

func (r *RuntimeFC) closeIO(ctx context.Context, ctrID, execID string) error {
	return nil
}
