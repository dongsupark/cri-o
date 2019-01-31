package oci

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
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

	//     fmt.Printf("entering CreateContainer\n")

	r.fcClient = r.newFireClient()

	c.state.Status = ContainerStateCreated

	return nil
}

func (r *RuntimeFC) startVM(c *Container) error {
	_ = r.fcCleanup()

	//     fmt.Printf("entering startVM\n")

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

	//     fmt.Printf("entering StartContainer\n")

	if err := r.startVM(c); err != nil {
		return err
	}

	c.state.Status = ContainerStateRunning

	return nil
}

// ExecContainer prepares a streaming endpoint to execute a command in the container.
func (r *RuntimeFC) ExecContainer(c *Container, cmd []string, stdin io.Reader, stdout, stderr io.WriteCloser, tty bool, resize <-chan remotecommand.TerminalSize) error {
	return nil
}

// ExecSyncContainer execs a command in a container and returns it's stdout, stderr and return code.
func (r *RuntimeFC) ExecSyncContainer(c *Container, command []string, timeout int64) (*ExecSyncResponse, error) {
	return &ExecSyncResponse{}, nil
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

	c.state.Status = ContainerStateStopped

	if err := waitContainerStop(r.ctx, c, killContainerTimeout, false); err != nil {
		return err
	}

	//     fmt.Printf("StopC: doing nothing\n")

	//     return r.stopVM()
	return nil
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

	if err := r.remove(r.ctx, c.ID(), ""); err != nil {
		return err
	}

	c.state.Status = ContainerStateStopped

	//     logrus.Infof("DeleteC: trying to run stopVM")
	//     fmt.Printf("DeleteC: trying to run stopVM\n")

	// NOTE: ignore error, since we should continue removing the container
	// on the cri-o side, even if stopVM could not kill the firecracker process.
	for i := 0; i < 3; i++ {
		if err := r.stopVM(); err != nil {
			logrus.Warnf("stopVM failed, but continue removing the container: %v", err)
			fmt.Printf("stopVM failed, but continue removing the container: %v\n", err)
			time.Sleep(500 * time.Millisecond)
		} else {
			break
		}
	}
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

	c.state.Status = ContainerStatePaused

	return nil
}

// UnpauseContainer unpauses a container.
func (r *RuntimeFC) UnpauseContainer(c *Container) error {
	// Lock the container
	c.opLock.Lock()
	defer c.opLock.Unlock()

	c.state.Status = ContainerStateRunning

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
	return int32(0), time.Now(), nil
}

func (r *RuntimeFC) kill(ctx context.Context, ctrID, execID string, signal syscall.Signal, all bool) error {
	return nil
}

func (r *RuntimeFC) remove(ctx context.Context, ctrID, execID string) error {
	return nil
}
