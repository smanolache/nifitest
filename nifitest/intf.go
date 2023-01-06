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

type Tester interface {
	Run(flowId string, testData map[string]string,
		expected map[string]string) error
}
