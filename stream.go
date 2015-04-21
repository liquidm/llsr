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
)

var (
	ErrStreamAlreadyRunning     = errors.New("llsr: Replication stream is already opened")
	ErrUnableToReadWholeMessage = errors.New("Unable to read whole message")
)

type LogPos int64

type Stream struct {
	cmd     *exec.Cmd
	running bool

	stdOut io.ReadCloser
	stdErr io.ReadCloser

	errFifo  *Fifo
	dataFifo *Fifo

	msgChan chan *RowMessage

	finished     chan error
	runtimeError error
}

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
	stream := &Stream{
		cmd:      cmd,
		finished: make(chan error),
		errFifo: NewFifo(),
		dataFifo: NewFifo(),
		msgChan: make(chan *RowMessage),
	}
	go stream.convertData()
	return stream
}

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

	s.errFifo.Open()
	s.dataFifo.Open()

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

func (s *Stream) Stop() error {
	return s.cmd.Process.Signal(os.Interrupt)
}

func (s *Stream) Finished() <-chan error {
	return s.finished
}

func (s *Stream) Data() <-chan *RowMessage {
	return s.msgChan
}

func (s *Stream) ErrOut() <-chan interface{} {
	return s.errFifo.Output()
}

func (s *Stream) recvErrors() {
	reader := bufio.NewReader(s.stdErr)
	for {
		str, err := reader.ReadString('\n')
		if len(str) > 0 {
			s.errFifo.Input() <- str
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
		n, err := s.stdOut.Read(data)
		if err != nil {
			s.stopWith(err)
			return
		}
		if n != int(length+1) {
			s.stopWith(ErrUnableToReadWholeMessage)
			return
		}
		s.dataFifo.Input() <- data[:length]
	}
}

func (s *Stream) convertData() {
	for {
		data := (<-s.dataFifo.Output()).([]byte)
		decodedData := &RowMessage{}

		err := proto.Unmarshal(data, decodedData)
		if err != nil {
			s.stopWith(err)
			return
		}

		s.msgChan<- decodedData
	}
}

func (s *Stream) wait() {
	err := s.cmd.Wait()
	s.errFifo.Close()
	s.dataFifo.Close()
	if err == nil {
		err = s.runtimeError
		s.runtimeError = nil
	}
	s.finished <- err
}

func (s *Stream) stopWith(err error) {
	s.runtimeError = err
	stopErr := s.Stop()
	if stopErr != nil {
		select {
		case s.finished <- err:
		default:
		}
	}
}
