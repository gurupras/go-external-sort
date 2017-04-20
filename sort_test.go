package extsort

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"

	"gopkg.in/vmihailenco/msgpack.v2"

	"github.com/gurupras/go-easyfiles"
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

func TestIntSort(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	var err error
	var chunks []string
	var memory int = 1048576

	merge_out_channel := make(chan SortInterface, 10000)

	callback := func(channel chan SortInterface, sort_params SortParams, quit chan bool) {
		var expected_fstruct *easyfiles.File
		if expected_fstruct, err = easyfiles.Open("./test/sort_expected.gz", os.O_RDONLY, easyfiles.GZ_TRUE); err != nil {
			return
		}
		defer expected_fstruct.Close()

		expected_chan := make(chan string, 10000)
		asyncReadLine(expected_fstruct, expected_chan)

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
			lines++
			if lines%10000 == 0 {
				//fmt.Println("Finished comparing:", lines)
			}
			assert.Equal(expected, object.String())
		}
		quit <- true
	}

	if chunks, err = ExternalSort("./test/sort.gz", memory, IntSortParams); err != nil {
		assert.Fail("Failed to run external sort", err)
	}
	//fmt.Println("Merging...")
	NWayMergeGenerator(chunks, IntSortParams, merge_out_channel, callback)
	for _, chunk := range chunks {
		_ = chunk
		os.Remove(chunk)
	}
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
