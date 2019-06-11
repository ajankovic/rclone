// Manage background jobs that the rc is running

package jobs

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ncw/rclone/fs/accounting"
	"github.com/ncw/rclone/fs/rc"
	"github.com/pkg/errors"
)

const (
	// expire the job when it is finished and older than this
	expireDuration = 60 * time.Second
	// inteval to run the expire cache
	expireInterval = 10 * time.Second
)

// Job describes a asynchronous task started via the rc package
type Job struct {
	mu        sync.Mutex
	ID        int64           `json:"id"`
	StartTime time.Time       `json:"startTime"`
	EndTime   time.Time       `json:"endTime"`
	Error     string          `json:"error"`
	Finished  bool            `json:"finished"`
	Success   bool            `json:"success"`
	Duration  float64         `json:"duration"`
	Output    rc.Params       `json:"output"`
	Context   context.Context `json:"-"`
	Stop      func()          `json:"-"`
}

// Jobs describes a collection of running tasks
type Jobs struct {
	mu             sync.RWMutex
	jobs           map[int64]*Job
	expireInterval time.Duration
	expireRunning  bool
}

var (
	running = newJobs()
	jobID   = int64(0)
)

// newJobs makes a new Jobs structure
func newJobs() *Jobs {
	return &Jobs{
		jobs:           map[int64]*Job{},
		expireInterval: expireInterval,
	}
}

// kickExpire makes sure Expire is running
func (jobs *Jobs) kickExpire() {
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	if !jobs.expireRunning {
		time.AfterFunc(jobs.expireInterval, jobs.Expire)
		jobs.expireRunning = true
	}
}

// Expire expires any jobs that haven't been collected
func (jobs *Jobs) Expire() {
	jobs.mu.Lock()
	defer jobs.mu.Unlock()
	now := time.Now()
	for ID, job := range jobs.jobs {
		job.mu.Lock()
		job.Stop()
		if job.Finished && now.Sub(job.EndTime) > expireDuration {
			delete(jobs.jobs, ID)
		}
		job.mu.Unlock()
	}
	if len(jobs.jobs) != 0 {
		time.AfterFunc(jobs.expireInterval, jobs.Expire)
		jobs.expireRunning = true
	} else {
		jobs.expireRunning = false
	}
}

// IDs returns the IDs of the running jobs
func (jobs *Jobs) IDs() (IDs []int64) {
	jobs.mu.RLock()
	defer jobs.mu.RUnlock()
	IDs = []int64{}
	for ID := range jobs.jobs {
		IDs = append(IDs, ID)
	}
	return IDs
}

// Get a job with a given ID or nil if it doesn't exist
func (jobs *Jobs) Get(ID int64) *Job {
	jobs.mu.RLock()
	defer jobs.mu.RUnlock()
	return jobs.jobs[ID]
}

// mark the job as finished
func (job *Job) finish(out rc.Params, err error) {
	job.mu.Lock()
	job.EndTime = time.Now()
	if out == nil {
		out = make(rc.Params)
	}
	job.Output = out
	job.Duration = job.EndTime.Sub(job.StartTime).Seconds()
	if err != nil {
		job.Error = err.Error()
		job.Success = false
	} else {
		job.Error = ""
		job.Success = true
	}
	job.Finished = true
	job.mu.Unlock()
	running.kickExpire() // make sure this job gets expired
}

// run the job until completion writing the return status
func (job *Job) run(fn rc.Func, in rc.Params) {
	defer func() {
		if r := recover(); r != nil {
			job.finish(nil, errors.Errorf("panic received: %v", r))
		}
	}()
	job.finish(fn(job.Context, in))
}

// NewJob start a new Job off
func (jobs *Jobs) NewJob(fn rc.Func, in rc.Params) *Job {
	id := atomic.AddInt64(&jobID, 1)
	ctx, cancel := context.WithCancel(accounting.WithTransferGroup(
		context.Background(), transferGroup(id)))
	stop := func() {
		cancel()
		// Wait for cancel to propagate before returning.
		<-ctx.Done()
	}
	job := &Job{
		ID:        id,
		StartTime: time.Now(),
		Context:   ctx,
		Stop:      stop,
	}
	go job.run(fn, in)
	jobs.mu.Lock()
	jobs.jobs[job.ID] = job
	jobs.mu.Unlock()
	return job
}

func transferGroup(id int64) string {
	return fmt.Sprintf("job/%d", id)
}

// StartJob starts a new job and returns a Param suitable for output
func StartJob(fn rc.Func, in rc.Params) (rc.Params, error) {
	job := running.NewJob(fn, in)
	out := make(rc.Params)
	out["jobid"] = job.ID
	return out, nil
}

func init() {
	rc.Add(rc.Call{
		Path:  "job/status",
		Fn:    rcJobStatus,
		Title: "Reads the status of the job ID",
		Help: `Parameters
- jobid - id of the job (integer)

Results
- finished - boolean
- duration - time in seconds that the job ran for
- endTime - time the job finished (eg "2018-10-26T18:50:20.528746884+01:00")
- error - error from the job or empty string for no error
- finished - boolean whether the job has finished or not
- id - as passed in above
- startTime - time the job started (eg "2018-10-26T18:50:20.528336039+01:00")
- success - boolean - true for success false otherwise
- output - output of the job as would have been returned if called synchronously
- progress - output of the progress related to the underlying job
`,
	})
}

// Returns the status of a job
func rcJobStatus(ctx context.Context, in rc.Params) (out rc.Params, err error) {
	jobID, err := in.GetInt64("jobid")
	if err != nil {
		return nil, err
	}
	job := running.Get(jobID)
	if job == nil {
		return nil, errors.New("job not found")
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	out = make(rc.Params)
	err = rc.Reshape(&out, job)
	if err != nil {
		return nil, errors.Wrap(err, "reshape failed in job status")
	}
	return out, nil
}

func init() {
	rc.Add(rc.Call{
		Path:  "job/list",
		Fn:    rcJobList,
		Title: "Lists the IDs of the running jobs",
		Help: `Parameters - None

Results
- jobids - array of integer job ids
`,
	})
}

// Returns list of job ids.
func rcJobList(ctx context.Context, in rc.Params) (out rc.Params, err error) {
	out = make(rc.Params)
	out["jobids"] = running.IDs()
	return out, nil
}

func init() {
	rc.Add(rc.Call{
		Path:  "job/stop",
		Fn:    rcJobStop,
		Title: "Stop the running job",
		Help: `Parameters
- jobid - id of the job (integer)
`,
	})
}

// Stops the running job.
func rcJobStop(ctx context.Context, in rc.Params) (out rc.Params, err error) {
	jobID, err := in.GetInt64("jobid")
	if err != nil {
		return nil, err
	}
	job := running.Get(jobID)
	if job == nil {
		return nil, errors.New("job not found")
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	out = make(rc.Params)
	job.Stop()
	return out, nil
}

func init() {
	rc.Add(rc.Call{
		Path:  "job/transfers",
		Fn:    rcJobTransfers,
		Title: "Get statuses of file transfers for the job",
		Help: `Parameters
- jobid - id of the job (integer)

Results
- transfers - list of transfer status for files related to the job
`,
	})
}

// Lists all file transfers for the provided jobid.
func rcJobTransfers(ctx context.Context, in rc.Params) (out rc.Params, err error) {
	jobID, err := in.GetInt64("jobid")
	if err != nil {
		return nil, err
	}
	job := running.Get(jobID)
	if job == nil {
		return nil, errors.New("job not found")
	}

	stats := accounting.Stats.GroupStats(transferGroup(jobID))
	return rc.Params{
		"transfers": stats,
	}, nil
}
