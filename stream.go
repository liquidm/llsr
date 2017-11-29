package llsr

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/golang/protobuf/proto"
	"github.com/liquidm/llsr/decoderbufs"
)

var (
	ErrStreamAlreadyRunning     = errors.New("llsr: Replication stream is already opened")
	ErrUnableToReadWholeMessage = errors.New("llsr: Unable to read whole message")
)

//Stream represents pg_recvlogical process.
//It is low level object and you should generally use service.Client.
type Stream struct {
	cmd     *exec.Cmd
	running bool

	stdOut io.ReadCloser
	stdErr io.ReadCloser

	errEvents  chan interface{}
	dataEvents chan interface{}

	msgChan chan *decoderbufs.RowMessage

	finished     chan error
	runtimeError error
}

//Creates new Stream object
func NewStream(dbConfig *DatabaseConfig, slot string, startPos LogPos) *Stream {
	cmd := exec.Command("pg_recvlogical", "--start", "--file=-", "-S", slot, "-d", dbConfig.Database, "-F", "0")
	if len(dbConfig.User) > 0 {
		cmd.Args = append(cmd.Args, "-U", dbConfig.User)
	}
	if len(dbConfig.Host) > 0 {
		cmd.Args = append(cmd.Args, "-h", dbConfig.Host)
	}
	if dbConfig.Port > 0 {
		cmd.Args = append(cmd.Args, "-p", string(dbConfig.Port))
	}
	if len(dbConfig.Password) > 0 {
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", dbConfig.Password))
	}
	if startPos > 0 {
		cmd.Args = append(cmd.Args, "-I", startPos.String())
	}
	stream := &Stream{
		cmd:        cmd,
		finished:   make(chan error),
		errEvents:  make(chan interface{}),
		dataEvents: make(chan interface{}),
		msgChan:    make(chan *decoderbufs.RowMessage),
	}
	return stream
}

//Establishes connection with PostgreSQL LLSR
func (s *Stream) Start() error {
	var err error

	if s.running {
		return ErrStreamAlreadyRunning
	}

	s.stdOut, err = s.cmd.StdoutPipe()
	if err != nil {
		return err
	}

	s.stdErr, err = s.cmd.StderrPipe()
	if err != nil {
		return err
	}

	err = s.cmd.Start()
	if err != nil {
		return err
	}

	s.running = true

	go s.recvErrors()
	go s.recvData()
	go s.convertData()
	go s.wait()

	return nil
}

//Closes connection with LLSR. It does not block. You should wait on Finished channel to ensure stream is closed.
func (s *Stream) Close() error {
	return s.cmd.Process.Signal(os.Interrupt)
}

//Finished channel produces error message when underlying pg_recvlogical exits with error.
//It produces nil when pg_recvlogical exits with 0 (e.g when Close() was called)
func (s *Stream) Finished() <-chan error {
	return s.finished
}

//Data channel produces RowMessage objects.
func (s *Stream) Data() <-chan *decoderbufs.RowMessage {
	return s.msgChan
}

//ErrOut channel produces STDERR messages of underlying pg_recvlogical process.
func (s *Stream) ErrOut() <-chan interface{} {
	return s.errEvents
}

func (s *Stream) recvErrors() {
	reader := bufio.NewReader(s.stdErr)
	for {
		str, err := reader.ReadString('\n')
		if len(str) > 0 {
			s.errEvents <- str
		}
		if err == io.EOF {
			return
		}
		if err != nil {
			s.stopWith(err)
			return
		}
	}
}

func (s *Stream) recvData() {
	for {
		var length uint64
		err := binary.Read(s.stdOut, binary.BigEndian, &length)
		if err != nil {
			if err == io.EOF {
				return
			}
			s.stopWith(err)
			return
		}
		data := make([]byte, length+1)

		_, err = io.ReadFull(s.stdOut, data)
		if err != nil {
			s.stopWith(err)
			return
		}

		s.dataEvents <- data[:length]
	}
}

func (s *Stream) convertData() {
	for {
		data := (<-s.dataEvents).([]byte)
		decodedData := &decoderbufs.RowMessage{}

		err := proto.Unmarshal(data, decodedData)
		if err != nil {
			s.stopWith(err)
			return
		}

		s.msgChan <- decodedData
	}
}

func (s *Stream) wait() {
	err := s.cmd.Wait()
	if err == nil {
		err = s.runtimeError
		s.runtimeError = nil
	}
	s.finished <- err
}

func (s *Stream) stopWith(err error) {
	s.runtimeError = err
	stopErr := s.Close()
	if stopErr != nil {
		select {
		case s.finished <- err:
		default:
		}
	}
}
