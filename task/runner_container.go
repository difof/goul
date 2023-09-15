package task

import "context"

type RunnerContainer struct {
	Configs   []*TaskConfig
	Callbacks []Handler
	Runners   []*TaskRunner
}

func NewRunnerContainer() *RunnerContainer {
	return &RunnerContainer{
		Configs:   make([]*TaskConfig, 0),
		Callbacks: make([]Handler, 0),
		Runners:   make([]*TaskRunner, 0),
	}
}

func (r *RunnerContainer) Append(cfg *TaskConfig, cb Handler) error {
	r.Configs = append(r.Configs, cfg)
	r.Callbacks = append(r.Callbacks, cb)

	return nil
}

func (r *RunnerContainer) StartAll() error {
	return r.StartAllContext(context.Background())
}

func (r *RunnerContainer) StartAllContext(ctx context.Context) error {
	for i, cfg := range r.Configs {
		runner, err := cfg.DoContext(ctx, r.Callbacks[i])
		if err != nil {
			return err
		}

		r.Runners = append(r.Runners, runner)
	}

	return nil
}

func (r *RunnerContainer) CloseAll() error {
	for _, runner := range r.Runners {
		if err := runner.Close(); err != nil {
			return err
		}
	}

	return nil
}
