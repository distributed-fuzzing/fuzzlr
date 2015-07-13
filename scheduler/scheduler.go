package scheduler

import (
	"fmt"
	"log"
	"time"

	"github.com/golang/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
)

type Scheduler struct {
	mesos.ExecutorInfo
	seqn     uint64
	shutdown chan struct{}
	done     chan struct{}
}

func New(bin, corpus string, artifactURIs ...string) *Scheduler {
	return &Scheduler{
		ExecutorInfo: mesos.ExecutorInfo{
			ExecutorId: &mesos.ExecutorID{Value: proto.String("fuzzlr-executor")},
			Command: &mesos.CommandInfo{
				Value: proto.String("TODO"), // TODO
				Uris:  commandURIs(artifactURIs...),
			},
			Name: proto.String("Fuzzer"),
		},
		shutdown: make(chan struct{}),
		done:     make(chan struct{}, 1),
	}
}

func NewFuzzMasterExecutorInfo(os string, host string, port int, corpus string) *mesos.ExecutorInfo {
	cmd := fmt.Sprintf("mkdir -p work/corpus; unzip %s-%s -d ./work/corpus; "+
		"./go-fuzz-%s -workdir=work -master=%s:%d > master.log", corpus, os, os, host, port)
	return &mesos.ExecutorInfo{
		ExecutorId: &mesos.ExecutorID{Value: proto.String("fuzzlr-executor")},
		Command: &mesos.CommandInfo{
			Value: proto.String(cmd),
			Uris:  commandURIs(artifactURIs...),
		},
		Name: proto.String("Fuzzer"),
	}
}

func NewSlaveExecutorInfo(masterHost string, masterPort int, bin string) *mesos.ExecutorInfo {
	cmd := fmt.Sprintf("./go-fuzz -bin=%s -slave=%s:%d -v=1 > slave.log",
		bin, masterHost, masterPort)
	return &mesos.ExecutorInfo{
		ExecutorId: &mesos.ExecutorID{Value: proto.String("fuzzlr-executor")},
		Command: &mesos.CommandInfo{
			Value: proto.String(cmd),
			Uris:  commandURIs(artifactURIs...),
		},
		Name: proto.String("Fuzzer"),
	}
}

// Shutdown shuts down the scheduler or times out after a given duration.
func (s *Scheduler) Shutdown(timeout time.Duration) error {
	close(s.shutdown)
	select {
	case <-s.done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("scheduler: shutdown timed out after %s", timeout)
	}
}

//
// Mesos scheduler callbacks
//

func (s *Scheduler) Registered(_ sched.SchedulerDriver, frameworkID *mesos.FrameworkID,
	masterInfo *mesos.MasterInfo) {
	log.Printf("Framework %s registered with master %s", frameworkID, masterInfo)
}

func (s *Scheduler) Reregistered(_ sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Printf("Framework re-registered with master %s", masterInfo)
}

func (s *Scheduler) Disconnected(sched.SchedulerDriver) {
	log.Println("Framework disconnected with master")
}

func (s *Scheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	log.Printf("Received %d resource offers", len(offers))
}

func (s *Scheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Printf("Received task status [%s] for task [%s]", status.State, status.TaskId.Value)
}

func (s *Scheduler) FrameworkMessage(driver sched.SchedulerDriver, executorID *mesos.ExecutorID,
	slaveID *mesos.SlaveID, message string) {
	log.Printf("Received a framework message %q for: %s", message, executorID.Value)
}

func (s *Scheduler) OfferRescinded(_ sched.SchedulerDriver, offerID *mesos.OfferID) {
	log.Printf("Offer %s rescinded", offerID)
}

func (s *Scheduler) SlaveLost(_ sched.SchedulerDriver, slaveID *mesos.SlaveID) {
	log.Printf("Slave %s lost", slaveID)
}

func (s *Scheduler) ExecutorLost(_ sched.SchedulerDriver, executorID *mesos.ExecutorID, slaveID *mesos.SlaveID, status int) {
	log.Printf("Executor %s on slave %s was lost", executorID, slaveID)
}

func (s *Scheduler) Error(_ sched.SchedulerDriver, err string) {
	log.Printf("Receiving an error: %s", err)
}

func (s *Scheduler) newTask(id uint64, offer *mesos.Offer) *mesos.TaskInfo {
	const maxMem, maxCpus = 2.0, 1024.0
	cpus, mem := offerCpusAndMem(offer)
	cpus, mem := math.Min(cpus, maxCpus), math.Min(mem, maxMem)

	name := proto.String(fmt.Sprintf("fuzzlr-", id))
	return &mesos.TaskInfo{
		Executor: &s.ExecutorInfo,
		Name:     name,
		Resources: []*mesos.Resource{
			mesosutil.NewScalarResource("cpus", cpus),
			mesosutil.NewScalarResource("mem", mem),
		},
		SlaveId: offer.SlaveId,
		TaskId:  &mesos.TaskID{Value: name},
	}
}

// maxTasksForOffer computes how many tasks can be launched using a given offer
func offerCpusAndMem(offer *mesos.Offer) (float64, float64) {
	var cpus, mem float64

	for _, resource := range offer.Resources {
		switch resource.GetName() {
		case "cpus":
			cpus += *resource.GetScalar().Value
		case "mem":
			mem += *resource.GetScalar().Value
		}
	}

	return cpus, mem
}

func commandURIs(paths ...string) []*mesos.CommandInfo_URI {
	uris := make([]*mesos.CommandInfo_URI, len(paths))
	for i, path := range paths {
		path := path
		uris[i] = &mesos.CommandInfo_URI{Value: &path}
	}
	return uris
}
