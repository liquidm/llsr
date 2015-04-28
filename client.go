package llsr

//Converter is used to conver raw RowMessage structs into app specific data.
type Converter interface {
	//Converts RowMessage into app specific data.
	Convert(*RowMessage, EnumsMap) interface{}
}

type Client interface {
	Updates() <-chan interface{}
	Events() <-chan *Event
	Close()
}

type client struct {
	updates chan interface{}
	events  chan *Event

	dbConfig      *DatabaseConfig
	slot          string
	startPosition LogPos
	converter     Converter

	stream *Stream

	stopped    bool
	closeChan  chan struct{}
	closedChan chan bool

	enums EnumsMap
}

//Creates new Client struct
func NewClient(dbConfig *DatabaseConfig, converter Converter, slot string, startPosition LogPos) (Client, error) {
	enums, err := loadEnums(dbConfig)
	if err != nil {
		return nil, err
	}

	client := &client{
		dbConfig:      dbConfig,
		converter:     converter,
		slot:          slot,
		startPosition: startPosition,
		updates:       make(chan interface{}),
		events:        make(chan *Event),
		closeChan:     make(chan struct{}),
		closedChan:    make(chan bool),
		enums:         enums,
	}

	return client, client.start()
}

//Updates produces objects converted by Converter interface.
func (c *client) Updates() <-chan interface{} {
	return c.updates
}

//Events produces control events about underlying Stream object.
func (c *client) Events() <-chan *Event {
	return c.events
}

//Starts client. It does not block.
func (c *client) start() error {
	if c.stream != nil {
		return ErrStreamAlreadyRunning
	}

	c.stopped = false

	c.stream = NewStream(c.dbConfig, c.slot, c.startPosition)
	if err := c.stream.Start(); err != nil {
		return err
	}

	go c.recvData()
	go c.recvStdErr()
	go c.recvControl()

	return nil
}

//Stops client. It blocks untill pg_recvlogical closes.
func (c *client) Close() {
	c.stopped = true
	close(c.closeChan)
	<-c.closedChan
}

func (c *client) recvData() {
	for {
		select {
		case data := <-c.stream.Data():
			c.updates <- c.converter.Convert(data, c.enums)
			c.startPosition = LogPos(data.GetLogPosition())
		case <-c.closeChan:
			return
		}
	}
}

func (c *client) recvStdErr() {
	for {
		select {
		case stdErrStr := <-c.stream.ErrOut():
			c.events <- &Event{Type: EventBackendStdErr, Value: stdErrStr}
		case <-c.closeChan:
			return
		}
	}
}

func (c *client) recvControl() {
	for {
		select {
		case <-c.closeChan:
			c.stream.Close()
		case err := <-c.stream.Finished():
			if err != nil {
				go func() {
					c.events <- &Event{Type: EventBackendInvalidExitStatus, Value: err}
				}()
			}
			if !c.stopped {
				defer c.reconnect()
			} else {
				c.closedChan <- true
			}
			return
		}
	}
}

func (c *client) reconnect() {
	go func() {
		c.events <- &Event{Type: EventReconnect}
	}()
	c.stream = nil
	c.start()
}
