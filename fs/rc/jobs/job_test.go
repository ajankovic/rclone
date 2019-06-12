package jobs

import (
	"context"
	"io/ioutil"
	"os"
	"runtime"
	"testing"
	"time"

	_ "github.com/ncw/rclone/backend/local"
	"github.com/ncw/rclone/fs/accounting"
	_ "github.com/ncw/rclone/fs/operations"
	"github.com/ncw/rclone/fs/rc"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewJobs(t *testing.T) {
	jobs := newJobs()
	assert.Equal(t, 0, len(jobs.jobs))
}

func TestJobsKickExpire(t *testing.T) {
	jobs := newJobs()
	jobs.expireInterval = time.Millisecond
	assert.Equal(t, false, jobs.expireRunning)
	jobs.kickExpire()
	jobs.mu.Lock()
	assert.Equal(t, true, jobs.expireRunning)
	jobs.mu.Unlock()
	time.Sleep(10 * time.Millisecond)
	jobs.mu.Lock()
	assert.Equal(t, false, jobs.expireRunning)
	jobs.mu.Unlock()
}

func TestJobsExpire(t *testing.T) {
	wait := make(chan struct{})
	jobs := newJobs()
	jobs.expireInterval = time.Millisecond
	assert.Equal(t, false, jobs.expireRunning)
	job := jobs.NewJob(func(ctx context.Context, in rc.Params) (rc.Params, error) {
		defer close(wait)
		return in, nil
	}, rc.Params{})
	<-wait
	assert.Equal(t, 1, len(jobs.jobs))
	jobs.Expire()
	assert.Equal(t, 1, len(jobs.jobs))
	jobs.mu.Lock()
	job.EndTime = time.Now().Add(-expireDuration - 60*time.Second)
	assert.Equal(t, true, jobs.expireRunning)
	jobs.mu.Unlock()
	time.Sleep(10 * time.Millisecond)
	jobs.mu.Lock()
	assert.Equal(t, false, jobs.expireRunning)
	assert.Equal(t, 0, len(jobs.jobs))
	jobs.mu.Unlock()
}

var noopFn = func(ctx context.Context, in rc.Params) (rc.Params, error) {
	return nil, nil
}

func TestJobsIDs(t *testing.T) {
	jobs := newJobs()
	job1 := jobs.NewJob(noopFn, rc.Params{})
	job2 := jobs.NewJob(noopFn, rc.Params{})
	wantIDs := []int64{job1.ID, job2.ID}
	gotIDs := jobs.IDs()
	require.Equal(t, 2, len(gotIDs))
	if gotIDs[0] != wantIDs[0] {
		gotIDs[0], gotIDs[1] = gotIDs[1], gotIDs[0]
	}
	assert.Equal(t, wantIDs, gotIDs)
}

func TestJobsGet(t *testing.T) {
	jobs := newJobs()
	job := jobs.NewJob(noopFn, rc.Params{})
	assert.Equal(t, job, jobs.Get(job.ID))
	assert.Nil(t, jobs.Get(123123123123))
}

var longFn = func(ctx context.Context, in rc.Params) (rc.Params, error) {
	time.Sleep(1 * time.Hour)
	return nil, nil
}

var ctxFn = func(ctx context.Context, in rc.Params) (rc.Params, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

const (
	sleepTime      = 100 * time.Millisecond
	floatSleepTime = float64(sleepTime) / 1E9 / 2
)

// sleep for some time so job.Duration is non-0
func sleepJob() {
	time.Sleep(sleepTime)
}

func TestJobFinish(t *testing.T) {
	jobs := newJobs()
	job := jobs.NewJob(longFn, rc.Params{})
	sleepJob()

	assert.Equal(t, true, job.EndTime.IsZero())
	assert.Equal(t, rc.Params(nil), job.Output)
	assert.Equal(t, 0.0, job.Duration)
	assert.Equal(t, "", job.Error)
	assert.Equal(t, false, job.Success)
	assert.Equal(t, false, job.Finished)

	wantOut := rc.Params{"a": 1}
	job.finish(wantOut, nil)

	assert.Equal(t, false, job.EndTime.IsZero())
	assert.Equal(t, wantOut, job.Output)
	assert.True(t, job.Duration >= floatSleepTime)
	assert.Equal(t, "", job.Error)
	assert.Equal(t, true, job.Success)
	assert.Equal(t, true, job.Finished)

	job = jobs.NewJob(longFn, rc.Params{})
	sleepJob()
	job.finish(nil, nil)

	assert.Equal(t, false, job.EndTime.IsZero())
	assert.Equal(t, rc.Params{}, job.Output)
	assert.True(t, job.Duration >= floatSleepTime)
	assert.Equal(t, "", job.Error)
	assert.Equal(t, true, job.Success)
	assert.Equal(t, true, job.Finished)

	job = jobs.NewJob(longFn, rc.Params{})
	sleepJob()
	job.finish(wantOut, errors.New("potato"))

	assert.Equal(t, false, job.EndTime.IsZero())
	assert.Equal(t, wantOut, job.Output)
	assert.True(t, job.Duration >= floatSleepTime)
	assert.Equal(t, "potato", job.Error)
	assert.Equal(t, false, job.Success)
	assert.Equal(t, true, job.Finished)
}

// We've tested the functionality of run() already as it is
// part of NewJob, now just test the panic catching
func TestJobRunPanic(t *testing.T) {
	wait := make(chan struct{})
	boom := func(ctx context.Context, in rc.Params) (rc.Params, error) {
		sleepJob()
		defer close(wait)
		panic("boom")
	}

	jobs := newJobs()
	job := jobs.NewJob(boom, rc.Params{})
	<-wait
	runtime.Gosched() // yield to make sure job is updated

	// Wait a short time for the panic to propagate
	for i := uint(0); i < 10; i++ {
		job.mu.Lock()
		e := job.Error
		job.mu.Unlock()
		if e != "" {
			break
		}
		time.Sleep(time.Millisecond << i)
	}

	job.mu.Lock()
	assert.Equal(t, false, job.EndTime.IsZero())
	assert.Equal(t, rc.Params{}, job.Output)
	assert.True(t, job.Duration >= floatSleepTime)
	assert.Equal(t, "panic received: boom", job.Error)
	assert.Equal(t, false, job.Success)
	assert.Equal(t, true, job.Finished)
	job.mu.Unlock()
}

func TestJobsNewJob(t *testing.T) {
	jobID = 0
	jobs := newJobs()
	job := jobs.NewJob(noopFn, rc.Params{})
	assert.Equal(t, int64(1), job.ID)
	assert.Equal(t, job, jobs.Get(1))
	assert.NotEmpty(t, job.Context)
}

func TestStartJob(t *testing.T) {
	jobID = 0
	out, err := StartJob(longFn, rc.Params{})
	assert.NoError(t, err)
	assert.Equal(t, rc.Params{"jobid": int64(1)}, out)
}

func TestRcJobStatus(t *testing.T) {
	jobID = 0
	_, err := StartJob(longFn, rc.Params{})
	assert.NoError(t, err)

	call := rc.Calls.Get("job/status")
	assert.NotNil(t, call)
	in := rc.Params{"jobid": 1}
	out, err := call.Fn(context.Background(), in)
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, float64(1), out["id"])
	assert.Equal(t, "", out["error"])
	assert.Equal(t, false, out["finished"])
	assert.Equal(t, false, out["success"])

	in = rc.Params{"jobid": 123123123}
	_, err = call.Fn(context.Background(), in)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "job not found")

	in = rc.Params{"jobidx": 123123123}
	_, err = call.Fn(context.Background(), in)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Didn't find key")
}

func TestRcJobList(t *testing.T) {
	jobID = 0
	_, err := StartJob(longFn, rc.Params{})
	assert.NoError(t, err)

	call := rc.Calls.Get("job/list")
	assert.NotNil(t, call)
	in := rc.Params{}
	out, err := call.Fn(context.Background(), in)
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, rc.Params{"jobids": []int64{1}}, out)
}

func TestRcJobStop(t *testing.T) {
	jobID = 0
	_, err := StartJob(ctxFn, rc.Params{})
	assert.NoError(t, err)

	call := rc.Calls.Get("job/stop")
	assert.NotNil(t, call)
	in := rc.Params{"jobid": 1}
	out, err := call.Fn(context.Background(), in)
	require.NoError(t, err)
	require.Empty(t, out)

	in = rc.Params{"jobid": 123123123}
	_, err = call.Fn(context.Background(), in)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "job not found")

	in = rc.Params{"jobidx": 123123123}
	_, err = call.Fn(context.Background(), in)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Didn't find key")

	time.Sleep(10 * time.Millisecond)

	call = rc.Calls.Get("job/status")
	assert.NotNil(t, call)
	in = rc.Params{"jobid": 1}
	out, err = call.Fn(context.Background(), in)
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, float64(1), out["id"])
	assert.Equal(t, "context canceled", out["error"])
	assert.Equal(t, true, out["finished"])
	assert.Equal(t, false, out["success"])
}

func TestRcJobTransfers(t *testing.T) {
	jobID = 0
	call := rc.Calls.Get("job/transfers")
	assert.NotNil(t, call)

	t.Run("transfer groups not enabled", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "job-transfers-")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		copyfile, wait := wrapFn(rc.Calls.Get("operations/copyfile").Fn)
		_, err = StartJob(copyfile, rc.Params{
			"srcFs":     "testdata/",
			"srcRemote": "foo.txt",
			"dstFs":     dir,
			"dstRemote": "bar.txt",
		})
		assert.NoError(t, err)
		wait()

		in := rc.Params{"jobid": 1}
		out, err := call.Fn(context.Background(), in)
		require.NoError(t, err)
		var exp []accounting.FileStatus
		assert.Equal(t, rc.Params{"transfers": exp}, out)
	})

	t.Run("transfer groups enabled", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "job-transfers-")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		copyfile, wait := wrapFn(rc.Calls.Get("operations/copyfile").Fn)
		_, err = StartJob(copyfile, rc.Params{
			"srcFs":                "testdata/",
			"srcRemote":            "foo.txt",
			"dstFs":                dir,
			"dstRemote":            "bar.txt",
			"enableTransferGroups": true,
		})
		assert.NoError(t, err)
		wait()

		in := rc.Params{"jobid": 2}
		exp := []accounting.FileStatus{
			{
				Name: "foo.txt",
				Size: 3,
				Sent: 3,
			},
		}
		out, err := call.Fn(context.Background(), in)
		require.NoError(t, err)
		assert.Equal(t, rc.Params{"transfers": exp}, out)
	})

	t.Run("tg enabled file exists", func(t *testing.T) {
		copyfile, wait := wrapFn(rc.Calls.Get("operations/copyfile").Fn)
		_, err := StartJob(copyfile, rc.Params{
			"srcFs":                "testdata/",
			"srcRemote":            "foo.txt",
			"dstFs":                "testdata/",
			"dstRemote":            "bar.txt",
			"enableTransferGroups": true,
		})
		assert.NoError(t, err)
		wait()

		in := rc.Params{"jobid": 3}
		exp := []accounting.FileStatus{
			{
				Name: "foo.txt",
				Size: 3,
				Sent: 3,
			},
		}
		out, err := call.Fn(context.Background(), in)
		require.NoError(t, err)
		assert.Equal(t, rc.Params{"transfers": exp}, out)

	})
}

// wrap fn to create wait function that will return when the fn is complete.
func wrapFn(fn rc.Func) (rc.Func, func()) {
	done := make(chan struct{})
	return func(ctx context.Context, in rc.Params) (rc.Params, error) {
			defer close(done)
			return fn(ctx, in)
		}, func() {
			<-done
			time.Sleep(20 * time.Millisecond)
		}
}
