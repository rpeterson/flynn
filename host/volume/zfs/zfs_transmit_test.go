package zfs

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	. "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
	//	gzfs "github.com/flynn/flynn/Godeps/_workspace/src/github.com/mistifyio/go-zfs"
	"github.com/flynn/flynn/pkg/testutils"
)

type ZfsTransmitTests struct {
	pool1 TempZpool
	pool2 TempZpool
}

var _ = Suite(&ZfsTransmitTests{})

func (ZfsTransmitTests) SetUpSuite(c *C) {
	// Skip all tests in this suite if not running as root.
	// Many zfs operations require root priviledges.
	testutils.SkipIfNotRoot(c)
}

func (s *ZfsTransmitTests) SetUpTest(c *C) {
	s.pool1.SetUpTest(c)
	s.pool2.SetUpTest(c)
}

func (s *ZfsTransmitTests) TearDownTest(c *C) {
	s.pool1.TearDownTest(c)
	s.pool2.TearDownTest(c)
}

/*
	Testing behaviors of 'zfs send' & 'zfs recv' in isolation to make sure deltas work the way we expect.

	See integration tests for taking the full trip over the wire through the REST API.
*/
func (s *ZfsTransmitTests) TestZfsSendRecvFull(c *C) {
	// create volume; add content; snapshot it.
	// note that 'zfs send' refuses anything but snapshots.
	v, err := s.pool1.VolProv.NewVolume()
	c.Assert(err, IsNil)
	f, err := os.Create(filepath.Join(v.Location(), "alpha"))
	c.Assert(err, IsNil)
	f.Close()
	snap, err := s.pool1.VolProv.CreateSnapshot(v)
	c.Assert(err, IsNil)

	var buf bytes.Buffer
	s.pool1.VolProv.SendSnapshot(snap, &buf)

	// send stream to this pool; should get a new snapshot volume
	snapRestored, err := s.pool1.VolProv.ReceiveSnapshot(bytes.NewBuffer(buf.Bytes()))
	c.Assert(err, IsNil)
	c.Assert(snapRestored.IsSnapshot(), Equals, true)
	// check that contents came across
	c.Assert(snapRestored.Location(), testutils.DirContains, []string{"alpha"})

	// send stream to another pool; should get a new volume
	snapRestored, err = s.pool2.VolProv.ReceiveSnapshot(bytes.NewBuffer(buf.Bytes()))
	c.Assert(err, IsNil)
	c.Assert(snapRestored.IsSnapshot(), Equals, true)
	// check that contents came across
	c.Assert(snapRestored.Location(), testutils.DirContains, []string{"alpha"})
}
