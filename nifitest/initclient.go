package nifitest

import (
	"net/url"
	"net/http"
	"crypto/tls"
	"crypto/rand"
)

func getHTTPClient(scheme string, proxyURL string, verifySrvCert bool) (
	*http.Client, error) {

	transport, err := getTransport(scheme, proxyURL, verifySrvCert)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: transport}, nil
}

func getTransport(scheme string, proxyURL string, verifySrvCert bool) (
	*http.Transport, error) {

	pxy, err := getProxy(proxyURL)
	if err != nil {
		return nil, err
	}
	tls := getTLS(scheme, verifySrvCert)
	return &http.Transport{
		Proxy:           pxy,
		TLSClientConfig: tls,
	}, nil
}

func getProxy(proxyURL string) (func (*http.Request) (*url.URL, error), error) {
	if proxyURL != "" {
		url, err := url.ParseRequestURI(proxyURL)
		if err != nil {
			return nil, err
		}
		return http.ProxyURL(url), nil
	}
	return nil, nil
}

func getTLS(scheme string, verifySrvCert bool) *tls.Config {
	if scheme == "https" {
		return &tls.Config{
			Rand:               rand.Reader,
			InsecureSkipVerify: !verifySrvCert,
		}
	}
	return nil
}
