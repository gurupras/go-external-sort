package extsort

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/gurupras/go-easyfiles"

	log "github.com/sirupsen/logrus"
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
	outChan := make(chan string, 100)
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
	var outfile *easyfiles.Writer

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
		//log.Infof("Saving to chunk: %v", outfile_path)
		if outfile_raw, err = sort_params.FSInterface.Open(outfile_path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, easyfiles.GZ_TRUE); err != nil {
			log.Warnf("Failed to open chunk: %v: %v", outfile_path, err)
			return
		}
		log.Infof("Writing chunk: %v (%d lines)", outfile_path, len(sort_params.Lines))

		if outfile, err = outfile_raw.Writer(bufsize); err != nil {
			log.Warnf("Failed to get writer to chunk: %v: %v", outfile_path, err)
			return
		}
		chunks = append(chunks, outfile_path)

		var b []byte
		// First write the number of objects
		if b, err = msgpack.Marshal(len(sort_params.Lines)); err != nil {
			log.Warnf("Failed msgpack.Marshal on num objects: %v", err)
			chunks = nil
			return
		}
		if _, err = outfile.Write(b); err != nil {
			log.Warnf("Failed write: %v", err)
			chunks = nil
			return
		}

		numBytes := 0
		for _, object := range sort_params.Lines {
			if b, err = msgpack.Marshal(object); err != nil {
				log.Warnf("Failed msgpack.Marshal on object: %v", err)
				chunks = nil
				return
			}
			if _, err = outfile.Write(b); err != nil {
				log.Fatalf("Failed to write data to file: %v: %v", outfile_path, err)
			}
			numBytes += len(b)
		}
		log.Infof("Saving to chunk: %v (%vb)", outfile_path, numBytes)
		chunk_idx += 1
		if err = outfile.Flush(); err != nil {
			log.Fatalf("Failed flush: %v: %v", outfile_path, err)
		}
		if err = outfile.Close(); err != nil {
			log.Fatalf("Failed writer close: %v: %v", outfile_path, err)
		}
		if err = outfile_raw.Close(); err != nil {
			log.Fatalf("Failed close: %v: %v", outfile_path, err)
		}

		// XXX: Remove this
		// Check the written data
		/*
			var info os.FileInfo
			info, err = sort_params.FSInterface.Stat(outfile_path)
			if err != nil {
				log.Warnf("Failed stat")
				return
			}
			if info.Size() == 0 {
				log.Warnf("Stat returned 0: %v", outfile_path)
				return
			} else {
				log.Infof("stat returned: %v", info.Size())
			}
		*/
	}
	//fmt.Fprintln(os.Stderr, fmt.Sprintf("%s: %d lines", file, lines))
	log.Infof("Total lines read while splitting: %v", lines)
	return
}

func NWayMergeGenerator(chunks []string, sort_params SortParams, kwargs ...map[string]interface{}) (<-chan SortInterface, error) {

	var readers map[string]io.Reader = make(map[string]io.Reader)
	var channels map[string]chan SortInterface = make(map[string]chan SortInterface)
	var err error

	var channelSize int
	if len(kwargs) > 0 {
		if v, ok := kwargs[0]["channel_size"]; ok {
			if channelSize, ok = v.(int); !ok {
				log.Errorf("Expected channel_size type to be int. Got %t", v)
			}
		}
	}
	outChan := make(chan SortInterface, channelSize)

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
		defer close(outChan)
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
				/*
					for _, v := range loglines {
						if v != nil {
							log.Fatalf("Breaking from loop when non-nil value in loglines")
						}
					}
				*/
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
				close(outChan)
			}
			outChan <- next_line
			//log.Infof("Dumped line to outChan")
			lines_read++
		}
		log.Infof("Total lines from all chunks: %v", lines_read)
	}

	go func() {
		// Set up readers and channels
		log.Infof("Setting up readers for %d chunks", len(chunks))
		wg := sync.WaitGroup{}
		files := make([]*easyfiles.File, len(chunks))
		for idx, chunk := range chunks {
			chunk_file, err := sort_params.FSInterface.Open(chunk, os.O_RDONLY, easyfiles.GZ_TRUE)
			if err != nil {
				err = fmt.Errorf("Failed to open file: %v: %v", chunk, err)
				log.Fatalf("%v", err)
			}
			files[idx] = chunk_file
			reader, err := chunk_file.RawReader()
			if err != nil {
				err = fmt.Errorf("Failed to get reader to file: %v: %v", chunk, err)
				log.Fatalf("%v", err)
			}
			readers[chunk] = reader
			// Resize channel size based on number of channels
			channels[chunk] = make(chan SortInterface, 1000)
		}
		// Start the producers
		for idx, _ := range chunks {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				defer files[idx].Close()
				producer(idx)
			}(idx)
		}

		// Start consumer
		log.Infof("Starting consumer")
		go consumer()

		wg.Wait()
	}()
	return outChan, err
}
