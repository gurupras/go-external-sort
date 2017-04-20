package extsort

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"sort"

	"github.com/gurupras/go-easyfiles"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type SortInterface interface {
	String() string
	Less(s SortInterface) (bool, error)
}

type SortCollection []SortInterface

type SortParams struct {
	Instance    func() SortInterface
	LineConvert func(string) SortInterface
	Lines       SortCollection
	FSInterface easyfiles.FileSystemInterface
}

func (s SortCollection) Len() int {
	return len(s)
}

func (s SortCollection) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortCollection) Less(i, j int) bool {
	if s[i] != nil {
		if ret, err := s[i].Less(s[j]); err == nil {
			return ret
		} else {
			// We can't really send this up. So panic
			panic(fmt.Sprintf("Failed to sort:%v", err))
		}
	} else {
		return false
	}
}

func asyncReadLine(file *easyfiles.File) <-chan string {
	outChan := make(chan string, 10000)
	go func() {
		defer close(outChan)
		reader, err := file.Reader(0)
		if err != nil {
			fmt.Fprintln(os.Stderr, fmt.Sprintf("Failed to get reader to file '%v': %v", file.Path, err))
		}
		reader.Split(bufio.ScanLines)
		for reader.Scan() {
			outChan <- reader.Text()
		}
	}()
	return outChan
}

func ExternalSort(file string, bufsize int, sort_params SortParams) (chunks []string, err error) {
	var fstruct *easyfiles.File

	var outfile_path string
	var outfile_raw *easyfiles.File
	var outfile easyfiles.Writer

	chunk_idx := 0
	bytes_read := 0

	//fmt.Printf("Splitting '%s' into chunks\n", file)

	if fstruct, err = sort_params.FSInterface.Open(file, os.O_RDONLY, easyfiles.GZ_UNKNOWN); err != nil {
		return
	}
	defer fstruct.Close()

	idx := 0
	lines := 0

	inputChannel := asyncReadLine(fstruct)

	for {
		sort_params.Lines = sort_params.Lines[:0]
		bytes_read = 0
		for {
			line, ok := <-inputChannel
			if !ok {
				break
			}

			if object := sort_params.LineConvert(line); object != nil {
				sort_params.Lines = append(sort_params.Lines, object)
				bytes_read += len(line)
			} else {
				fmt.Fprintln(os.Stderr, fmt.Sprintf("Failed to parse line(%d): \n%s\n", line, idx))
			}
			lines++
			if lines%1000 == 0 {
				//fmt.Println("Lines:", lines)
			}
			if bytes_read > bufsize {
				break
			}
		}
		if len(sort_params.Lines) == 0 {
			// We got no lines in the last iteration..break
			break
		}

		sort.Sort(sort_params.Lines)

		outfile_path = fmt.Sprintf("%s.chunk.%08d.gz", file, chunk_idx)
		//fmt.Println("Saving to chunk:", outfile_path)
		if outfile_raw, err = sort_params.FSInterface.Open(outfile_path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, easyfiles.GZ_TRUE); err != nil {
			return
		}
		defer outfile_raw.Close()

		if outfile, err = outfile_raw.Writer(0); err != nil {
			return
		}
		chunks = append(chunks, outfile_path)

		var b []byte
		// First write the number of objects
		if b, err = msgpack.Marshal(len(sort_params.Lines)); err != nil {
			chunks = nil
			return
		}
		if _, err = outfile.Write(b); err != nil {
			chunks = nil
			return
		}

		for _, object := range sort_params.Lines {
			if b, err = msgpack.Marshal(object); err != nil {
				chunks = nil
				return
			}
			outfile.Write(b)
		}
		chunk_idx += 1
		outfile.Flush()
		outfile.Close()
	}
	//fmt.Fprintln(os.Stderr, fmt.Sprintf("%s: %d lines", file, lines))
	return
}

func NWayMergeGenerator(chunks []string, sort_params SortParams, out_channel chan SortInterface,
	callback func(out_channel chan SortInterface, sort_params SortParams, quit chan bool)) error {

	var readers map[string]io.Reader = make(map[string]io.Reader)
	var channels map[string]chan SortInterface = make(map[string]chan SortInterface)
	var err error
	quit := make(chan bool, 1)

	// Read file and write to channel
	closed_channels := 0
	producer := func(idx int) {
		chunk := chunks[idx]
		reader := readers[chunk]
		channel := channels[chunk]

		// Get number of objects
		decoder := msgpack.NewDecoder(reader)
		var numObjects int
		if err := decoder.Decode(&numObjects); err != nil {
			log.Fatalf("Failed to read number of objects from file: %v", chunk)
		}

		for idx := 0; idx < numObjects; idx++ {
			s := sort_params.Instance()
			if err := decoder.Decode(s); err != nil {
				log.Fatalf("Failed to decode into SortInterface: %v", err)
			}
			channel <- s
			//fmt.Println("CHANNEL-%d: %s", idx, line)
		}
		//fmt.Println("Closing channel:", idx, ":", lines)
		closed_channels++
		close(channel)
	}

	// Now for the consumer
	consumer := func() {
		defer close(out_channel)

		loglines := make([]SortInterface, len(chunks))

		lines_read := 0
		for {
			var more bool = false
			for idx, chunk := range chunks {
				channel := channels[chunk]
				if loglines[idx] == nil {
					logline, ok := <-channel
					if !ok {
						continue
					}
					loglines[idx] = logline
					more = true
				} else {
					// This index was not nil. This implies we have more
					more = true
				}
			}
			if more == false {
				break
			}

			min := loglines[0]
			min_idx := 0
			for idx, logline := range loglines {
				if logline != nil {
					if less, err := logline.Less(min); err != nil {
						fmt.Fprintln(os.Stderr, fmt.Sprintf("Failed to compare lines: \n%v\n%v\n", logline.String(), min.String()))
						os.Exit(-1)
					} else {
						if less {
							min = logline
							min_idx = idx
						}
					}
				}
			}
			next_line := loglines[min_idx]
			loglines[min_idx] = nil

			if next_line == nil {
				fmt.Fprintln(os.Stderr, "Attempting to write nil to file..The loop should've broken before this point")
				fmt.Fprintln(os.Stderr, "Open channels:", (len(chunks) - closed_channels))
				fmt.Fprintln(os.Stderr, "lines read:", lines_read)
				for idx, l := range loglines {
					if l != nil {
						fmt.Fprintln(os.Stderr, idx, ":", l.String())
					}
				}
				close(out_channel)
			}
			out_channel <- next_line
		}
	}

	// Set up readers and channels
	for _, chunk := range chunks {
		chunk_file, err := sort_params.FSInterface.Open(chunk, os.O_RDONLY, easyfiles.GZ_TRUE)
		if err != nil {
			goto out
		}
		defer chunk_file.Close()
		reader, err := chunk_file.RawReader()
		if err != nil {
			goto out
		}
		readers[chunk] = reader
		// Resize channel size based on number of channels
		NORMAL_SIZE := 10000
		channel_size := NORMAL_SIZE / len(chunks)
		channels[chunk] = make(chan SortInterface, channel_size)
	}

	// Start the producers
	for idx, _ := range chunks {
		go producer(idx)
	}

	// Start consumer
	go consumer()

	go callback(out_channel, sort_params, quit)
	//fmt.Println("NWayMergeGenerator: Waiting for callback to quit")
	_ = <-quit
out:
	return err
}
