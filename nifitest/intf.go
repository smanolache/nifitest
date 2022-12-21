package nifitest

import(
	"github.com/antihax/optional"
)

type Id interface {
	Id()      string
	GroupId() string
}

type Config struct {
	URL                     string
	Proxy                   string
	Username                optional.String
	Password                optional.String
	VerifyServerCertificate bool
}

type Tester interface {
	Run(flowId string, testData map[Id]string,
		expected map[Id]string) error
}
