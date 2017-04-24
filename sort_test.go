package extsort

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"gopkg.in/vmihailenco/msgpack.v2"

	"github.com/gurupras/go-easyfiles"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Int int

func (a *Int) String() string {
	return strconv.Itoa(int(*a))
}

func (l *Int) Less(s SortInterface) (ret bool, err error) {
	var o *Int
	var ok bool
	if s != nil {
		if o, ok = s.(*Int); !ok {
			err = errors.New(fmt.Sprintf("Failed to convert from SortInterface to *Int:", reflect.TypeOf(s)))
			ret = false
			goto out
		}
	}
	if l != nil && o != nil {
		ret = int(*l) < int(*o)
	} else if l != nil {
		ret = true
	} else {
		ret = false
	}
out:
	return
}

type IntSlice []*Int

func ParseInt(line string) SortInterface {
	if ret, err := strconv.Atoi(line); err != nil {
		return nil
	} else {
		retval := Int(ret)
		return &retval
	}
}

var IntSortParams SortParams = SortParams{
	Instance: func() SortInterface {
		var v Int
		return &v
	},
	LineConvert: ParseInt,
	Lines:       make(SortCollection, 0),
	FSInterface: easyfiles.LocalFS,
}

func TestIntString(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	i := 5
	I := Int(i)
	str := I.String()

	assert.Equal("5", str, "Should be equal")
}

func testIntSort(t *testing.T, name string) {
	t.Parallel()

	assert := assert.New(t)
	require := require.New(t)

	var err error
	var chunks []string
	var memory int = 1024 * 128

	callback := func(channel <-chan SortInterface, sort_params SortParams) {
		var expected_fstruct *easyfiles.File
		if expected_fstruct, err = easyfiles.Open(fmt.Sprintf("./test/%v_expected.gz", name), os.O_RDONLY, easyfiles.GZ_TRUE); err != nil {
			return
		}
		defer expected_fstruct.Close()

		expected_chan := asyncReadLine(expected_fstruct)

		var expected string
		var object SortInterface
		var ok bool
		var lines int64 = 0
		for {
			if expected, ok = <-expected_chan; !ok {
				if _, ok = <-channel; !ok {
					// Both streams are done
					break
				} else {
					assert.Fail("Expected file ended while n-way merge generator still has data?")
				}
			} else {
				if object, ok = <-channel; !ok || object == nil {
					assert.Fail("Expected file has data while n-way merge generator has ended?")
				}
			}
			//log.Infof("expected=%v object=%v", expected, object.String())
			lines++
			if lines%100 == 0 {
				//log.Infof("Compared: %v", lines)
			}
			require.Equal(expected, object.String())
		}
	}

	if chunks, err = ExternalSort(fmt.Sprintf("./test/%v.gz", name), memory, IntSortParams); err != nil {
		assert.Fail("Failed to run external sort", fmt.Sprintf("%v", err))
	}
	log.Infof("Created %v chunks. Merging...", len(chunks))
	outChan, err := NWayMergeGenerator(chunks, IntSortParams)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		callback(outChan, IntSortParams)
	}()
	wg.Wait()
	for _, chunk := range chunks {
		_ = chunk
		os.Remove(chunk)
	}
}

func TestShortIntSort(t *testing.T) {
	testIntSort(t, "sort_small")
}

func TestLongIntSort(t *testing.T) {
	testIntSort(t, "sort")
}

type I interface {
	String() string
}

type container struct {
	A string
	B string
	C int
}

func (c *container) String() string {
	return fmt.Sprintf("A=%v B=%v C=%v", c.A, c.B, c.C)
}

func TestMessagePackDecode(t *testing.T) {
	require := require.New(t)

	obj1 := []string{"hello", "haha"}

	obj2 := I((&(container{"a", "b", 67})))

	buf := bytes.NewBuffer(nil)

	encoder := msgpack.NewEncoder(buf)
	err := encoder.Encode(obj1)
	require.Nil(err)

	err = encoder.Encode(obj2)
	require.Nil(err)

	got := bytes.NewBuffer(buf.Bytes())
	decoder := msgpack.NewDecoder(got)

	var gotObj1 []string
	var gotObj2 container
	err = decoder.Decode(&gotObj1)
	require.Nil(err)

	err = decoder.Decode(&gotObj2)
	require.Nil(err)

	require.True(reflect.DeepEqual(obj1, gotObj1))
	require.True(reflect.DeepEqual(obj2, I(&gotObj2)), fmt.Sprintf("expected: %v\ngot: %v\n", obj2, gotObj2))
}
