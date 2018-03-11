package internal

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

//IMPORTANT: This test will create local files in internal_testdata/
//make sure  *_testdata is in gitinore.
const workdir = "internal_testdata"
const logfile = workdir + "/my_log"

func TestBasicTracking(t *testing.T) {
	os.Mkdir(workdir, 466)
	os.Remove(logfile)

	tracker, err := NewTransferTracker(logfile)

	assert.NoError(t, err, "unexpected error")

	istrans, err := tracker.IsTransferredAndTrackIfNot("file1", 10)
	assert.NoError(t, err, "unexpected error")
	assert.False(t, istrans, "must be fales, i.e. not in the log")

	err = tracker.TrackFileTransferComplete("file1")
	assert.NoError(t, err, "unexpected error")

	err = tracker.TrackTransferComplete()
	assert.NoError(t, err, "unexpected error")
	os.Remove(logfile)

}

func TestBasicTrackingAndResume(t *testing.T) {
	os.Mkdir(workdir, 0666)
	os.Remove(logfile)

	tracker, err := NewTransferTracker(logfile)

	assert.NoError(t, err, "unexpected error")

	istrans, err := tracker.IsTransferredAndTrackIfNot("file1", 10)
	assert.NoError(t, err, "unexpected error")
	assert.False(t, istrans, "must be false, i.e. not in the log")

	istrans, err = tracker.IsTransferredAndTrackIfNot("file2", 10)
	assert.NoError(t, err, "unexpected error")
	assert.False(t, istrans, "must be false, i.e. not in the log")

	//only one succeeds.
	err = tracker.TrackFileTransferComplete("file1")
	assert.NoError(t, err, "unexpected error")

	//closing the handle manually as the program will continue (handle to the file will exists) and need to simulate a crash
	err = tracker.loghandle.Close()
	assert.NoError(t, err, "unexpected error")

	//create another instance of a tracker at this point only one file should be transferred.
	tracker, err = NewTransferTracker(logfile)
	assert.NoError(t, err, "unexpected error")

	istrans, err = tracker.IsTransferredAndTrackIfNot("file1", 10)
	assert.NoError(t, err, "unexpected error")
	assert.True(t, istrans, "must be true, as this file was signaled as succesfully transferred")

	istrans, err = tracker.IsTransferredAndTrackIfNot("file2", 10)
	assert.NoError(t, err, "unexpected error")
	assert.False(t, istrans, "must be false, this was was not signaled as complete")

	err = tracker.TrackFileTransferComplete("file2")
	assert.NoError(t, err, "unexpected error")

	err = tracker.TrackTransferComplete()
	assert.NoError(t, err, "unexpected error")
	os.Remove(logfile)
}
