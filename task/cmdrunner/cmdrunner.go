package cmdrunner

type RunnerHandler func() error

// CmdRunner is a runner manager and only one instance per process is needed.
//
// Runners will be started as soon the CmdRunner.Start() is called, and will be
// automatically stopped if the stop signal is received. Starting and stopping of
// runners must be handled by user, using the provided handlers.
//
// Example:
//
//	r := NewCmdRunner()
type CmdRunner struct {
}

type Runner struct {
	startHandler RunnerHandler
	stopHandler  RunnerHandler
}

func NewCmdRunner() *CmdRunner {
	return &CmdRunner{}
}

// Wait for stop signal and stop all runners
func (c *CmdRunner) Wait() {
}

// Add a new runner
func (c *CmdRunner) Add(start, stop RunnerHandler) {

}
