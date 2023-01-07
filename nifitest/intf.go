package nifitest

import(
	"github.com/antihax/optional"
)

type Config struct {
	URL                     string
	Proxy                   string
	Username                optional.String
	Password                optional.String
	VerifyServerCertificate bool
	RemoveOutLinks          bool
}

/*
    --------------------------------------------
    |                                          |
    |                                          |
    |<-------------------                      |
    |                   |                      |
    v                   ^                      ^
   Idle ---------> Initialized -----------> Executing
    ^                   v                      v
    |                   |                      |
    |                   -------> Error <--------
    |                              v
    |                              |
    --------------------------------
*/
type State int
const (
	Idle State = iota
	Initialized
	Executing
	Done

	Error
)

type Tester interface {
	TestSync(testData, expected map[string]string) (bool, error)

	// If the bool value is set then the test has finished. The error may
	// be non-nil in this case. Either because there was an error during
	// the test (and then the bool is false) or there was an error when
	// rolling back the test components (the bool could be true in this
	// case, for example if the test has succeeded but the roll back has
	// failed). If the error is not nil then call 'Rollback' until it
	// returns no error.
	// If the bool value is not set then the flow execution has started but
	// has not yet finished. The error is always nil in this case. Call
	// 'Check' later.
	TestAsync(testData, expected map[string]string) (optional.Bool, error)
	Check() (optional.Bool, error)

	Rollback() error

	State() State
}
