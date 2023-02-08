package task

import (
	"log"
	"testing"
	"time"
)

func TestNewScheduler(t *testing.T) {
	s := NewScheduler(time.Millisecond)
	t.Log("starting scheduler")
	s.Start()
	time.Sleep(time.Second)

	taskFactory := func(name string) TaskHandler {
		return func(task *Task) error {
			log.Printf("Task %s (%s) - elapsed %s", name, task.Id(), task.Elapsed)

			if name == "stop-after-4s" {
				log.Printf("Stopping task %s (%s)", name, task.Id())
				task.Stop()
			}

			return nil
		}
	}

	t.Log("adding tasks")
	s.Once().Do(taskFactory("once"))
	s.Every(1).Seconds().Do(taskFactory("every-1s"))
	s.Every(2).Seconds().Do(taskFactory("every-2s"))
	s.Every(4).Seconds().Do(taskFactory("stop-after-4s"))
	s.Once().After(3 * time.Second).Do(taskFactory("once-after-3s"))

	t.Log("waiting for tasks to finish")
	time.Sleep(10 * time.Second)
	t.Log("stopping scheduler")
	s.Stop()
	t.Log("scheduler stopped")
	time.Sleep(time.Second)
}

func TestAfter(t *testing.T) {
	s := NewScheduler(time.Millisecond)
	t.Log("starting scheduler")
	s.Start()
	time.Sleep(time.Second)

	s.Every(1).Second().After(3 * time.Second).Do(func(task *Task) error {
		log.Printf("Task %s - elapsed %s", task.Id(), task.Elapsed)

		return nil
	})

	t.Log("waiting for tasks to finish")
	time.Sleep(5 * time.Second)
	t.Log("stopping scheduler")
	s.Stop()
	t.Log("scheduler stopped")
	time.Sleep(time.Second)
}
