package llsr

//Concurent FIFO data structure
type Fifo struct {
	write     chan interface{}
	read      chan interface{}
	first     *element
	last      *element
	opened    bool
	closeChan chan bool
}

type element struct {
	value    interface{}
	next     *element
	previous *element
}

//Creates new Fifo
func NewFifo() *Fifo {
	fifo := &Fifo{
		write:     make(chan interface{}),
		read:      make(chan interface{}),
		closeChan: make(chan bool),
	}
	return fifo
}

//Appends new element to Fifo.
//It blocks if fifo is closed.
func (f *Fifo) Input() chan<- interface{} {
	return f.write
}

//Gets next element from Fifo.
//It blocks if fifo is closed or fifo is empty.
func (f *Fifo) Output() <-chan interface{} {
	return f.read
}

//Makes Input and Output channels operable.
func (f *Fifo) Open() {
	if f.opened {
		return
	}
	f.opened = true
	go func() {
	mainLoop:
		for {
			outValue, found := f.get()
			if found {
				select {
				case inValue := <-f.write:
					f.add(inValue)
				case f.read <- outValue:
					f.remove()
				case <-f.closeChan:
					break mainLoop
				}
			} else {
				select {
				case inValue := <-f.write:
					f.add(inValue)
				case <-f.closeChan:
					break mainLoop
				}
			}
		}
		f.opened = false
		f.closeChan <- true
	}()
}

//Closes Fifo making Input and Output channels blocking.
func (f *Fifo) Close() {
	f.closeChan <- true
	<-f.closeChan
}

func (f *Fifo) add(v interface{}) {
	el := &element{value: v}
	if f.last == nil {
		f.first = el
		f.last = el
	} else {
		el.previous = f.last
		el.previous.next = el
		f.last = el
	}
}

func (f *Fifo) get() (interface{}, bool) {
	el := f.first
	if el == nil {
		return nil, false
	}

	return el.value, true
}

func (f *Fifo) remove() {
	el := f.first
	next := el.next
	if next != nil {
		f.first = next
		next.previous = nil
	} else {
		f.first = nil
		f.last = nil
	}
}
