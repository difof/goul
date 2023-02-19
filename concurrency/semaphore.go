package concurrency

import "sync"

type Semaphore struct {
	counter int
	max     int
	lock    sync.Mutex
}

func NewSemaphore(max int) *Semaphore {
	return &Semaphore{
		max: max,
	}
}

func (s *Semaphore) TryAcquire(n int) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.counter+n > s.max {
		return false
	}

	s.counter += n
	return true
}

func (s *Semaphore) Release(n int) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.counter < n {
		panic("semaphore: releasing more than acquired")
	}

	s.counter -= n
}
