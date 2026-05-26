/*
 * This file is part of the KubeVirt project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright The KubeVirt Authors.
 *
 */

package virtwrap

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
	"unsafe"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"

	backupv1 "kubevirt.io/api/backup/v1alpha1"
	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/log"

	cmdv1 "kubevirt.io/kubevirt/pkg/handler-launcher-com/cmd/v1"
	cmdclient "kubevirt.io/kubevirt/pkg/virt-handler/cmd-client"
	"kubevirt.io/kubevirt/pkg/virt-launcher/metadata"
	"kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/api"
	"kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/stats"
)

// DomainEventNotifier is the interface for sending domain events to virt-handler.
// This avoids an import cycle with the notify-client package.
type DomainEventNotifier interface {
	SendDomainEvent(event watch.Event) error
}

// FakeDomainManager implements DomainManager without libvirt/QEMU.
// It simulates VM lifecycle and migration for testing infrastructure
// concerns (networking, storage, scheduling) on clusters without
// hardware virtualization support.
//
// Core methods:
//   - SyncVMI: creates domain, starts `sleep infinity` process, writes PID file, emits Added event
//   - KillVMI/DeleteVMI/SignalShutdownVMI: sets Shutoff state, kills fake process, emits Modified event
//   - PauseVMI/UnpauseVMI: toggles Paused/Running state
//   - ListAllDomains: returns the fake domain (or empty before SyncVMI)
//   - MarkGracefulShutdownVMI: sets grace period metadata
//
// Migration methods:
//   - MigrateVMI (source): sets migration metadata, spawns goroutine that sleeps ~3s
//     then transitions to Shutoff/Migrated. The fake process is NOT killed here;
//     it is left running so virt-handler can process the domain state change before
//     the pod terminates. The normal KillVMI cleanup path handles process termination.
//   - PrepareMigrationTarget: creates domain on target, spawns goroutine that sleeps ~2s
//     then transitions to Running and starts a fake process.
//   - FinalizeVirtualMachineMigration: sends Gratuitous ARP on the target to announce
//     the VM's presence, allowing network agents (e.g., Calico Felix) to detect migration.
//   - CancelVMIMigration: sets abort status in migration metadata
//
// All other methods return zero values or "not supported in simulation mode".
type FakeDomainManager struct {
	mu             sync.Mutex
	domain         *api.Domain
	metadataCache  *metadata.Cache
	notifier       DomainEventNotifier
	events         chan watch.Event
	runWithNonRoot bool
	stopChan       chan struct{}

	// Fake process management
	fakeCmd *exec.Cmd
	fakePID int
	pidFile string

	// Migration idempotency: virt-handler may call MigrateVMI and
	// PrepareMigrationTarget multiple times during a single migration.
	// These flags ensure we only spawn one background goroutine per call.
	migrationStarted       bool
	targetPreparationDone  bool
}

// SimBuildIteration is incremented each time the code is rebuilt,
// so we can verify which version is running in the cluster.
const SimBuildIteration = 17

// NewFakeDomainManager creates a FakeDomainManager that simulates VM lifecycle.
func NewFakeDomainManager(
	metadataCache *metadata.Cache,
	runWithNonRoot bool,
	stopChan chan struct{},
) *FakeDomainManager {
	log.Log.Infof("Simulation mode: FakeDomainManager created (build iteration %d)", SimBuildIteration)
	return &FakeDomainManager{
		metadataCache:  metadataCache,
		runWithNonRoot: runWithNonRoot,
		stopChan:       stopChan,
	}
}

// SetNotifier sets the notifier used to send domain events to virt-handler.
func (f *FakeDomainManager) SetNotifier(notifier DomainEventNotifier) {
	f.notifier = notifier
}

// SetEventsChan sets the local events channel used by waitForDomainUUID
// and waitForFinalNotify in virt-launcher main().
func (f *FakeDomainManager) SetEventsChan(events chan watch.Event) {
	f.events = events
}

func (f *FakeDomainManager) pidDir() string {
	if f.runWithNonRoot {
		return "/run/libvirt/qemu/run"
	}
	return "/run/libvirt/qemu"
}

func (f *FakeDomainManager) startFakeProcess(domainName string) {
	cmd := exec.Command("sleep", "infinity")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		log.Log.Reason(err).Error("Failed to start fake process for simulation mode")
		return
	}
	f.fakeCmd = cmd
	f.fakePID = cmd.Process.Pid

	dir := f.pidDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Log.Reason(err).Errorf("Failed to create PID directory %s", dir)
		return
	}
	f.pidFile = filepath.Join(dir, domainName+".pid")
	if err := os.WriteFile(f.pidFile, []byte(strconv.Itoa(f.fakePID)), 0644); err != nil {
		log.Log.Reason(err).Errorf("Failed to write PID file %s", f.pidFile)
		return
	}

	// Reap the process in the background
	go cmd.Wait()

	log.Log.Infof("Simulation mode: started fake process PID %d, pidFile %s", f.fakePID, f.pidFile)
}

func (f *FakeDomainManager) killFakeProcess() {
	if f.fakeCmd != nil && f.fakeCmd.Process != nil {
		if err := syscall.Kill(f.fakePID, syscall.SIGTERM); err != nil {
			log.Log.Reason(err).Warningf("Failed to kill fake process PID %d", f.fakePID)
		}
		f.fakeCmd = nil
		f.fakePID = 0
	}
	if f.pidFile != "" {
		os.Remove(f.pidFile)
		f.pidFile = ""
	}
}

func (f *FakeDomainManager) emitEvent(eventType watch.EventType) {
	if f.domain == nil {
		return
	}
	domainCopy := f.domain.DeepCopy()

	// Debug: verify migration metadata in the copy
	if domainCopy.Spec.Metadata.KubeVirt.Migration != nil {
		log.Log.Infof("Simulation mode: emitEvent(%s) - domain has Migration metadata, EndTimestamp=%v",
			eventType, domainCopy.Spec.Metadata.KubeVirt.Migration.EndTimestamp)
	} else {
		log.Log.Infof("Simulation mode: emitEvent(%s) - domain has NO Migration metadata", eventType)
	}

	event := watch.Event{
		Type:   eventType,
		Object: domainCopy,
	}

	if f.notifier != nil {
		if err := f.notifier.SendDomainEvent(event); err != nil {
			log.Log.Reason(err).Error("Simulation mode: failed to send domain event via notifier")
		}
	}
	if f.events != nil {
		select {
		case f.events <- event:
		default:
			log.Log.Warning("Simulation mode: events channel full, dropping event")
		}
	}
}

// --- DomainManager interface implementation ---

func (f *FakeDomainManager) SyncVMI(vmi *v1.VirtualMachineInstance, allowEmulation bool, options *cmdv1.VirtualMachineOptions) (*api.DomainSpec, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	log.Log.Object(vmi).Info("Simulation mode: SyncVMI called")

	domainName := api.VMINamespaceKeyFunc(vmi)

	if f.domain == nil {
		f.domain = &api.Domain{}
		// ObjectMeta.Name must be the VMI name (not namespace_name) so the
		// domain cache key (namespace/name) matches the VMI informer key.
		f.domain.ObjectMeta.Name = vmi.Name
		f.domain.ObjectMeta.Namespace = vmi.Namespace
		f.domain.ObjectMeta.UID = vmi.UID
		// Spec.Name is the libvirt domain name (namespace_name format),
		// used for PID files and internal identification.
		f.domain.Spec.Name = domainName
		f.domain.Spec.UUID = string(vmi.UID)
		f.domain.Spec.Metadata.KubeVirt.UID = vmi.UID

		// Populate domain interfaces from VMI spec so virt-handler's
		// ifacesStatusFromDomainInterfaces can create interface status
		// entries with the correct Name (from alias) and MAC. These
		// entries are then matched by MAC with InterfacesStatus() data
		// to populate vmi.Status.Interfaces with IP addresses.
		for i, iface := range vmi.Spec.Domain.Devices.Interfaces {
			domainIface := api.Interface{
				Alias: api.NewUserDefinedAlias(iface.Name),
			}
			// Get MAC from VMI spec or from the pod's eth0 for the primary interface
			if iface.MacAddress != "" {
				domainIface.MAC = &api.MAC{MAC: iface.MacAddress}
			} else if i == 0 {
				// For the primary interface, use the pod's eth0 MAC
				if podIface, err := net.InterfaceByName("eth0"); err == nil {
					domainIface.MAC = &api.MAC{MAC: podIface.HardwareAddr.String()}
				}
			}
			f.domain.Spec.Devices.Interfaces = append(f.domain.Spec.Devices.Interfaces, domainIface)
		}

		// Set Status.Interfaces directly on the domain object so the
		// domain informer picks up the IP immediately (rather than
		// waiting for the 5-minute GetDomain resync).
		f.domain.Status.Interfaces = f.InterfacesStatus()

		f.metadataCache.UID.Set(vmi.UID)

		f.startFakeProcess(domainName)

		f.domain.SetState(api.Running, api.ReasonUnknown)
		f.emitEvent(watch.Added)

		// Send GARP to announce the VM's presence on the network
		if err := sendGARP(); err != nil {
			log.Log.Reason(err).Warning("Simulation mode: failed to send GARP on initial boot")
		}
	}

	return &f.domain.Spec, nil
}

func (f *FakeDomainManager) PauseVMI(vmi *v1.VirtualMachineInstance) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	log.Log.Object(vmi).Info("Simulation mode: PauseVMI called")
	if f.domain != nil {
		f.domain.SetState(api.Paused, api.ReasonPausedUser)
		f.emitEvent(watch.Modified)
	}
	return nil
}

func (f *FakeDomainManager) UnpauseVMI(vmi *v1.VirtualMachineInstance) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	log.Log.Object(vmi).Info("Simulation mode: UnpauseVMI called")
	if f.domain != nil {
		f.domain.SetState(api.Running, api.ReasonUnknown)
		f.emitEvent(watch.Modified)
	}
	return nil
}

func (f *FakeDomainManager) FreezeVMI(_ *v1.VirtualMachineInstance, _ int32) error {
	return nil
}

func (f *FakeDomainManager) UnfreezeVMI(_ *v1.VirtualMachineInstance) error {
	return nil
}

func (f *FakeDomainManager) ResetVMI(_ *v1.VirtualMachineInstance) error {
	return nil
}

func (f *FakeDomainManager) SoftRebootVMI(_ *v1.VirtualMachineInstance) error {
	return nil
}

func (f *FakeDomainManager) KillVMI(vmi *v1.VirtualMachineInstance) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	log.Log.Object(vmi).Info("Simulation mode: KillVMI called")

	if f.domain == nil {
		return nil
	}

	// Match real LibvirtDomainManager behavior: only destroy if domain is
	// Running, Paused, or Shutdown. If domain is already Shutoff (e.g.,
	// after migration with reason=Migrated), do nothing. This is critical
	// for migration: the VM controller calls deleteVM() -> DeleteDomain()
	// when it detects domainMigrated (Shutoff/Migrated), which eventually
	// calls KillVMI. If we changed the state here, we'd overwrite the
	// Migrated reason before the migration-source controller can process it.
	if f.domain.Status.Status == api.Shutoff {
		log.Log.Object(vmi).Info("Simulation mode: domain already shutoff, nothing to do")
		return nil
	}

	f.domain.SetState(api.Shutoff, api.ReasonDestroyed)
	now := metav1.Now()
	f.domain.ObjectMeta.DeletionTimestamp = &now

	f.killFakeProcess()
	f.emitEvent(watch.Modified)
	return nil
}

// DeleteVMI removes the domain definition. In real libvirt this calls
// virDomainUndefine which removes the domain XML but doesn't destroy the
// process. The domain must already be shutoff. After undefine, the domain
// no longer appears in ListAllDomains.
func (f *FakeDomainManager) DeleteVMI(vmi *v1.VirtualMachineInstance) error {
	f.mu.Lock()
	log.Log.Object(vmi).Info("Simulation mode: DeleteVMI called")

	if f.domain == nil {
		f.mu.Unlock()
		return nil
	}
	f.mu.Unlock()

	// Simulate the time a real libvirt virDomainUndefine + domain shutdown
	// takes. This gives the migration controllers time to process the
	// domain state and update the VMI status before the process exits.
	log.Log.Object(vmi).Info("Simulation mode: delaying DeleteVMI to simulate domain shutdown")
	select {
	case <-time.After(5 * time.Second):
	case <-f.stopChan:
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.domain == nil {
		return nil
	}

	// Set DeletionTimestamp so the domain is recognized as deleted
	now := metav1.Now()
	f.domain.ObjectMeta.DeletionTimestamp = &now
	f.emitEvent(watch.Modified)

	// Kill the fake process so ProcessMonitor detects the exit and
	// virt-launcher shuts down. Without this, the source pod stays
	// running indefinitely after migration because nothing else
	// terminates the fake process (KillVMI is a no-op when the
	// domain is already Shutoff/Migrated).
	f.killFakeProcess()
	return nil
}

func (f *FakeDomainManager) SignalShutdownVMI(vmi *v1.VirtualMachineInstance) error {
	return f.KillVMI(vmi)
}

func (f *FakeDomainManager) MarkGracefulShutdownVMI() {
	log.Log.Info("Simulation mode: MarkGracefulShutdownVMI called")
	gracePeriod := api.GracePeriodMetadata{
		MarkedForGracefulShutdown: boolPtr(true),
	}
	f.metadataCache.GracePeriod.Store(gracePeriod)
}

func (f *FakeDomainManager) ListAllDomains() ([]*api.Domain, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.domain == nil {
		return []*api.Domain{}, nil
	}

	// Debug: log migration metadata state during domain listing (used by resync)
	if f.domain.Spec.Metadata.KubeVirt.Migration != nil {
		log.Log.Infof("Simulation mode: ListAllDomains - domain has Migration metadata, EndTimestamp=%v",
			f.domain.Spec.Metadata.KubeVirt.Migration.EndTimestamp)
	}

	return []*api.Domain{f.domain.DeepCopy()}, nil
}

func (f *FakeDomainManager) MigrateVMI(vmi *v1.VirtualMachineInstance, _ *cmdclient.MigrationOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	log.Log.Object(vmi).Infof("Simulation mode: MigrateVMI (source) called, labels=%v annotations=%v", vmi.Labels, vmi.Annotations)
	if vmi.Labels["migration-timeout"] == "true" {
		log.Log.Object(vmi).Info("Simulation mode: migration-timeout label detected, will simulate timeout failure")
	}

	// Idempotency guard: virt-handler may call MigrateVMI multiple times
	// during a single migration (e.g., on re-enqueue). Only start the
	// background goroutine once.
	if f.migrationStarted {
		log.Log.Object(vmi).Info("Simulation mode: migration already started, skipping")
		return nil
	}
	f.migrationStarted = true

	// Initialize migration metadata - get UID from VMI status, same as real code
	migrationUID := vmi.Status.MigrationState.MigrationUID
	if vmi.Status.MigrationState.SourceState != nil {
		migrationUID = vmi.Status.MigrationState.SourceState.MigrationUID
	}
	now := metav1.Now()
	migrationMetadata := api.MigrationMetadata{
		UID:            migrationUID,
		StartTimestamp: &now,
		Mode:           v1.MigrationPreCopy,
	}
	f.metadataCache.Migration.Store(migrationMetadata)

	// Also set migration metadata on the domain object itself.
	// virt-handler's domain informer receives domain objects via events,
	// and the migration-source controller checks domain.Spec.Metadata.KubeVirt.Migration
	// for EndTimestamp to determine when migration is complete.
	f.domain.Spec.Metadata.KubeVirt.Migration = &migrationMetadata

	// Simulate migration in background — branch on timeout label.
	// The label originates from VM spec.template.metadata.labels and is
	// automatically propagated to the VMI by virt-controller.
	if vmi.Labels["migration-timeout"] == "true" {
		go f.simulateMigrationTimeout(vmi)
	} else {
		go f.simulateMigration(vmi)
	}
	return nil
}

func (f *FakeDomainManager) simulateMigration(vmi *v1.VirtualMachineInstance) {
	// Simulate migration taking ~3 seconds
	select {
	case <-time.After(3 * time.Second):
	case <-f.stopChan:
		return
	}

	// Mark migration as completed in metadata
	now := metav1.Now()
	f.metadataCache.Migration.WithSafeBlock(func(md *api.MigrationMetadata, initialized bool) {
		md.EndTimestamp = &now
	})

	// Transition source domain to Shutoff/Migrated.
	//
	// Important: do NOT kill the fake process here. In the real KubeVirt flow,
	// libvirt sets the domain state to Shutoff/Migrated first, virt-handler's
	// migration-source controller detects the state change and updates the VMI
	// status to mark migration as completed, and only then does the normal
	// cleanup path (KillVMI) terminate the process and pod.
	//
	// If we kill the fake process here, the ProcessMonitor detects the death
	// and shuts down virt-launcher immediately. virt-handler sees the pod die
	// before it processes the domain state change, and interprets it as a crash
	// ("Migration failed vmi shutdown during migration").
	f.mu.Lock()
	if f.domain != nil {
		f.domain.SetState(api.Shutoff, api.ReasonMigrated)
		// Set EndTimestamp on the domain's migration metadata so that
		// virt-handler's migration-source controller can detect completion.
		if f.domain.Spec.Metadata.KubeVirt.Migration != nil {
			f.domain.Spec.Metadata.KubeVirt.Migration.EndTimestamp = &now
		}
	}
	f.mu.Unlock()

	log.Log.Object(vmi).Info("Simulation mode: migration completed on source, domain shutoff (process kept alive for cleanup)")

	f.emitEvent(watch.Modified)
}

func (f *FakeDomainManager) simulateMigrationTimeout(vmi *v1.VirtualMachineInstance) {
	// Simulate migration running for a bit before timing out
	select {
	case <-time.After(3 * time.Second):
	case <-f.stopChan:
		return
	}

	now := metav1.Now()
	f.metadataCache.Migration.WithSafeBlock(func(md *api.MigrationMetadata, initialized bool) {
		md.EndTimestamp = &now
		md.Failed = true
		md.FailureReason = "Timeout detected"
	})

	// Set failure on domain metadata so virt-handler's
	// setMigrationProgressStatus() reads it and propagates to VMI status.
	// Domain stays Running (NOT Shutoff/Migrated) — same as real timeout.
	f.mu.Lock()
	if f.domain != nil && f.domain.Spec.Metadata.KubeVirt.Migration != nil {
		f.domain.Spec.Metadata.KubeVirt.Migration.EndTimestamp = &now
		f.domain.Spec.Metadata.KubeVirt.Migration.Failed = true
		f.domain.Spec.Metadata.KubeVirt.Migration.FailureReason = "Timeout detected"
	}
	f.mu.Unlock()

	log.Log.Object(vmi).Info("Simulation mode: migration timeout simulated")
	f.emitEvent(watch.Modified)
}

func (f *FakeDomainManager) PrepareMigrationTarget(vmi *v1.VirtualMachineInstance, allowEmulation bool, options *cmdv1.VirtualMachineOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	log.Log.Object(vmi).Info("Simulation mode: PrepareMigrationTarget called")

	// Idempotency guard: virt-handler may call PrepareMigrationTarget multiple
	// times during a single migration. Only set up the domain and start the
	// background goroutine once.
	if f.targetPreparationDone {
		log.Log.Object(vmi).Info("Simulation mode: target preparation already done, skipping")
		return nil
	}
	f.targetPreparationDone = true

	domainName := api.VMINamespaceKeyFunc(vmi)

	// Create domain on target
	f.domain = &api.Domain{}
	// ObjectMeta.Name must be the VMI name (not namespace_name) so the
	// domain cache key (namespace/name) matches the VMI informer key.
	f.domain.ObjectMeta.Name = vmi.Name
	f.domain.ObjectMeta.Namespace = vmi.Namespace
	f.domain.ObjectMeta.UID = vmi.UID
	// Spec.Name is the libvirt domain name (namespace_name format).
	f.domain.Spec.Name = domainName
	f.domain.Spec.UUID = string(vmi.UID)
	f.domain.Spec.Metadata.KubeVirt.UID = vmi.UID

	// Populate domain interfaces (same as SyncVMI)
	for i, iface := range vmi.Spec.Domain.Devices.Interfaces {
		domainIface := api.Interface{
			Alias: api.NewUserDefinedAlias(iface.Name),
		}
		if iface.MacAddress != "" {
			domainIface.MAC = &api.MAC{MAC: iface.MacAddress}
		} else if i == 0 {
			if podIface, err := net.InterfaceByName("eth0"); err == nil {
				domainIface.MAC = &api.MAC{MAC: podIface.HardwareAddr.String()}
			}
		}
		f.domain.Spec.Devices.Interfaces = append(f.domain.Spec.Devices.Interfaces, domainIface)
	}

	f.metadataCache.UID.Set(vmi.UID)

	// Initialize migration metadata on target
	now := metav1.Now()
	targetMigrationMeta := api.MigrationMetadata{
		UID:            vmi.Status.MigrationState.MigrationUID,
		StartTimestamp: &now,
		Mode:           v1.MigrationPreCopy,
	}
	f.metadataCache.Migration.Store(targetMigrationMeta)

	// Also set migration metadata on the domain object itself.
	// virt-handler's migration-target controller checks
	// domain.Spec.Metadata.KubeVirt.Migration.EndTimestamp to determine
	// when migration is complete and call ackMigrationCompletion.
	f.domain.Spec.Metadata.KubeVirt.Migration = &targetMigrationMeta

	// Simulate target receiving the VM in the background
	go f.simulateTargetReceive(vmi, domainName)
	return nil
}

func (f *FakeDomainManager) simulateTargetReceive(vmi *v1.VirtualMachineInstance, domainName string) {
	// Wait for simulated migration data transfer
	select {
	case <-time.After(2 * time.Second):
	case <-f.stopChan:
		return
	}

	f.mu.Lock()
	f.domain.SetState(api.Running, api.ReasonUnknown)
	f.startFakeProcess(domainName)

	// Set Status.Interfaces so the domain informer picks up the IP
	// immediately (same as in SyncVMI).
	f.domain.Status.Interfaces = f.InterfacesStatus()

	// Mark migration complete on target — set EndTimestamp in both the
	// metadata cache and on the domain object. The domain object is what
	// virt-handler's domain informer sees; the target controller's
	// updateStatus checks domain.Spec.Metadata.KubeVirt.Migration.EndTimestamp
	// to call ackMigrationCompletion, which sets vmi.Status.MigrationState.EndTimestamp,
	// which is required for migrationNeedsFinalization to return true.
	now := metav1.Now()
	if f.domain.Spec.Metadata.KubeVirt.Migration != nil {
		f.domain.Spec.Metadata.KubeVirt.Migration.EndTimestamp = &now
	}
	f.mu.Unlock()

	f.metadataCache.Migration.WithSafeBlock(func(md *api.MigrationMetadata, initialized bool) {
		md.EndTimestamp = &now
	})

	// Debug: log migration metadata state before emitting event
	f.mu.Lock()
	if f.domain.Spec.Metadata.KubeVirt.Migration != nil {
		log.Log.Object(vmi).Infof("Simulation mode: domain.Migration set - UID=%s, StartTimestamp=%v, EndTimestamp=%v",
			f.domain.Spec.Metadata.KubeVirt.Migration.UID,
			f.domain.Spec.Metadata.KubeVirt.Migration.StartTimestamp,
			f.domain.Spec.Metadata.KubeVirt.Migration.EndTimestamp)
	} else {
		log.Log.Object(vmi).Warning("Simulation mode: domain.Migration is NIL before emitting event!")
	}
	f.mu.Unlock()

	log.Log.Object(vmi).Info("Simulation mode: target received VM, domain running")

	// Note: GARP is NOT sent here. During PrepareMigrationTarget the pod's
	// network interface may not be fully up yet ("network is down"). Instead,
	// GARP is sent in FinalizeVirtualMachineMigration, which is called by
	// virt-handler after the migration is fully complete and the target pod
	// is the active one.

	f.emitEvent(watch.Added)
}

func (f *FakeDomainManager) GetDomainStats() (*stats.DomainStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.domain == nil {
		return &stats.DomainStats{}, nil
	}
	return &stats.DomainStats{
		Name: f.domain.Spec.Name,
		UUID: f.domain.Spec.UUID,
	}, nil
}

func (f *FakeDomainManager) CancelVMIMigration(vmi *v1.VirtualMachineInstance) error {
	log.Log.Object(vmi).Info("Simulation mode: CancelVMIMigration called")
	now := metav1.Now()
	f.metadataCache.Migration.WithSafeBlock(func(md *api.MigrationMetadata, initialized bool) {
		md.AbortStatus = string(v1.MigrationAbortSucceeded)
		md.EndTimestamp = &now
		md.Failed = true
		md.FailureReason = "Migration cancelled"
	})
	return nil
}

func (f *FakeDomainManager) FinalizeVirtualMachineMigration(vmi *v1.VirtualMachineInstance, options *cmdv1.VirtualMachineOptions) error {
	log.Log.Object(vmi).Info("Simulation mode: FinalizeVirtualMachineMigration called")

	// Send GARP to announce the VM's presence on the target node.
	// This is done here (not in PrepareMigrationTarget/simulateTargetReceive)
	// because the pod's network interface is guaranteed to be up at this point.
	// In the real KubeVirt flow, the VM resumes on the target and naturally
	// sends GARPs. Network agents like Calico Felix use the GARP to detect
	// that the VM has migrated and update their dataplane accordingly.
	if err := sendGARP(); err != nil {
		log.Log.Reason(err).Warning("Simulation mode: failed to send GARP after migration finalization")
	}

	return nil
}

// --- Stubs for unsupported operations ---

func (f *FakeDomainManager) GetGuestInfo() v1.VirtualMachineInstanceGuestAgentInfo {
	return v1.VirtualMachineInstanceGuestAgentInfo{}
}

func (f *FakeDomainManager) GetUsers() []v1.VirtualMachineInstanceGuestOSUser {
	return nil
}

func (f *FakeDomainManager) GetFilesystems() []v1.VirtualMachineInstanceFileSystem {
	return nil
}

func (f *FakeDomainManager) HotplugHostDevices(_ *v1.VirtualMachineInstance) error {
	return nil
}

func (f *FakeDomainManager) InterfacesStatus() []api.InterfaceStatus {
	// Report the pod's IP so virt-handler populates vmi.Status.Interfaces.
	// In real KubeVirt this comes from the QEMU guest agent; in simulation
	// mode we read the pod's network interfaces directly.
	ipIface, err := net.InterfaceByName("eth0")
	if err != nil {
		return nil
	}
	addrs, err := ipIface.Addrs()
	if err != nil {
		return nil
	}

	var ips []string
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}
		ips = append(ips, ipNet.IP.String())
	}
	if len(ips) == 0 {
		return nil
	}

	return []api.InterfaceStatus{
		{
			Mac:           ipIface.HardwareAddr.String(),
			Ip:            ips[0],
			IPs:           ips,
			InterfaceName: "eth0",
		},
	}
}

func (f *FakeDomainManager) GetGuestOSInfo() *api.GuestOSInfo {
	return nil
}

func (f *FakeDomainManager) Exec(_, _ string, _ []string, _ int32) (string, error) {
	return "", fmt.Errorf("not supported in simulation mode")
}

func (f *FakeDomainManager) GuestPing(_ string) error {
	return fmt.Errorf("not supported in simulation mode")
}

func (f *FakeDomainManager) MemoryDump(_ *v1.VirtualMachineInstance, _ string) error {
	return fmt.Errorf("not supported in simulation mode")
}

func (f *FakeDomainManager) BackupVirtualMachine(_ *v1.VirtualMachineInstance, _ *backupv1.BackupOptions) error {
	return fmt.Errorf("not supported in simulation mode")
}

func (f *FakeDomainManager) RedefineCheckpoint(_ *v1.VirtualMachineInstance, _ *backupv1.BackupCheckpoint) (bool, error) {
	return false, fmt.Errorf("not supported in simulation mode")
}

func (f *FakeDomainManager) GetQemuVersion() (string, error) {
	return "simulation-0.0.0", nil
}

func (f *FakeDomainManager) UpdateVCPUs(_ *v1.VirtualMachineInstance, _ *cmdv1.VirtualMachineOptions) error {
	return nil
}

func (f *FakeDomainManager) GetSEVInfo() (*v1.SEVPlatformInfo, error) {
	return nil, fmt.Errorf("not supported in simulation mode")
}

func (f *FakeDomainManager) GetLaunchMeasurement(_ *v1.VirtualMachineInstance) (*v1.SEVMeasurementInfo, error) {
	return nil, fmt.Errorf("not supported in simulation mode")
}

func (f *FakeDomainManager) InjectLaunchSecret(_ *v1.VirtualMachineInstance, _ *v1.SEVSecretOptions) error {
	return fmt.Errorf("not supported in simulation mode")
}

func (f *FakeDomainManager) UpdateGuestMemory(_ *v1.VirtualMachineInstance) error {
	return nil
}

func (f *FakeDomainManager) GetDomainDirtyRateStats(_ time.Duration) (*stats.DomainStatsDirtyRate, error) {
	return &stats.DomainStatsDirtyRate{}, nil
}

func (f *FakeDomainManager) GetScreenshot(_ *v1.VirtualMachineInstance) (*cmdv1.ScreenshotResponse, error) {
	return nil, fmt.Errorf("not supported in simulation mode")
}

// --- GARP support ---

// sendGARP sends a Gratuitous ARP to announce the pod's IP/MAC to the network.
// This simulates what a real VM does when it boots or resumes after migration,
// allowing network agents (e.g., Calico Felix) to detect the VM is ready.
//
// In bridge binding mode, the pod's network interfaces are reorganized:
//   - eth0: dummy interface (state DOWN, NOARP) — holds the pod IP
//   - eth0-nic: original veth (state UP) — bridge port, connected to host
//   - k6t-eth0: bridge device
//   - tap0: tap for QEMU
//
// We must get the IP from eth0 but send the packet on eth0-nic (the live veth).
func sendGARP() error {
	// Get the pod IP from eth0 (dummy interface that holds the address)
	ipIface, err := net.InterfaceByName("eth0")
	if err != nil {
		return fmt.Errorf("failed to get interface eth0: %v", err)
	}

	addrs, err := ipIface.Addrs()
	if err != nil {
		return fmt.Errorf("failed to get addresses for eth0: %v", err)
	}

	var srcIP net.IP
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}
		if ip4 := ipNet.IP.To4(); ip4 != nil {
			srcIP = ip4
			break
		}
	}
	if srcIP == nil {
		return fmt.Errorf("no IPv4 address found on eth0")
	}

	// Determine the send interface: use eth0-nic (the live veth) if it exists
	// (bridge binding mode), otherwise fall back to eth0 (no bridge).
	sendIfaceName := "eth0"
	sendIface, err := net.InterfaceByName("eth0-nic")
	if err != nil {
		// No bridge binding — use eth0 directly
		sendIface = ipIface
	} else {
		sendIfaceName = "eth0-nic"
	}

	srcMAC := sendIface.HardwareAddr

	// Build Gratuitous ARP packet (ARP reply announcing our IP/MAC)
	// Ethernet header (14 bytes): dst(6) + src(6) + ethertype(2)
	// ARP payload (28 bytes): htype(2) + ptype(2) + hlen(1) + plen(1) + oper(2) + sha(6) + spa(4) + tha(6) + tpa(4)
	pkt := make([]byte, 42)

	// Ethernet header
	copy(pkt[0:6], net.HardwareAddr{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}) // dst: broadcast
	copy(pkt[6:12], srcMAC)                                              // src: our MAC
	binary.BigEndian.PutUint16(pkt[12:14], 0x0806)                       // ethertype: ARP

	// ARP payload
	binary.BigEndian.PutUint16(pkt[14:16], 1)                              // hardware type: Ethernet
	binary.BigEndian.PutUint16(pkt[16:18], 0x0800)                         // protocol type: IPv4
	pkt[18] = 6                                                            // hardware address length
	pkt[19] = 4                                                            // protocol address length
	binary.BigEndian.PutUint16(pkt[20:22], 2)                              // operation: ARP reply
	copy(pkt[22:28], srcMAC)                                               // sender hardware address
	copy(pkt[28:32], srcIP.To4())                                          // sender protocol address
	copy(pkt[32:38], net.HardwareAddr{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}) // target hardware address
	copy(pkt[38:42], srcIP.To4())                                          // target protocol address (same as sender for GARP)

	// Open raw socket
	fd, err := syscall.Socket(syscall.AF_PACKET, syscall.SOCK_RAW, int(htons(syscall.ETH_P_ARP)))
	if err != nil {
		return fmt.Errorf("failed to open raw socket: %v", err)
	}
	defer syscall.Close(fd)

	// Build sockaddr_ll for sending
	addr := syscall.SockaddrLinklayer{
		Protocol: htons(syscall.ETH_P_ARP),
		Ifindex:  sendIface.Index,
		Halen:    6,
	}
	copy(addr.Addr[:6], net.HardwareAddr{0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	if err := syscall.Sendto(fd, pkt, 0, &addr); err != nil {
		return fmt.Errorf("failed to send GARP on %s: %v", sendIfaceName, err)
	}

	log.Log.Infof("Simulation mode: sent GARP on %s (IP=%s, MAC=%s)", sendIfaceName, srcIP, srcMAC)
	return nil
}

// htons converts a uint16 from host to network byte order.
func htons(v uint16) uint16 {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], v)
	return *(*uint16)(unsafe.Pointer(&buf[0]))
}

// --- Helpers ---

func boolPtr(b bool) *bool {
	return &b
}
