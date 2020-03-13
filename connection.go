package go_elasticsearch

import (
	"context"
	"net/http"
	"net/url"
	"time"
)

type OptionFunc func(*Client) error

type Client struct {
	c                 *http.Client
	url               string
	basicAuth         bool   // indicates whether to send HTTP Basic Auth credentials
	basicAuthUsername string // username for HTTP Basic Auth
	basicAuthPassword string // password for HTTP Basic Auth
	DefaultProtocol   string
	ConnectionTimeout time.Duration
}

func NewClient(options ...OptionFunc) (*Client, error) {
	c := &Client{
		c:                 http.DefaultClient,
		DefaultProtocol:   "http",
		ConnectionTimeout: 1 * time.Second,
	}
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func SetUrl(url string) OptionFunc {
	return func(client *Client) error {
		client.url = url
		return nil
	}
}

// SetBasicAuth can be used to specify the HTTP Basic Auth credentials to
// use when making HTTP requests to Elasticsearch.
func SetBasicAuth(username, password string) OptionFunc {
	return func(client *Client) error {
		client.basicAuth = true
		client.basicAuthUsername = username
		client.basicAuthPassword = password
		return nil
	}
}

// PerformRequestOptions must be passed into PerformRequest.
type PerformRequestOptions struct {
	Method      string
	Path        string
	Params      url.Values
	Body        interface{}
	ContentType string
}

//Performs HTTP request
// string method method name
// string url URL
// string requestBody request body
// bool raw if response body contains JSON and should be decoded
func (this *Client) httpRequest(ctx context.Context, method, path string, params url.Values, requestBody interface{}, raw bool) (*Response, error) {
	pathWithParams := path
	if len(params) > 0 {
		pathWithParams += "?" + params.Encode()
	}
	request, err := NewRequest(method, this.url+pathWithParams)
	if err != nil {
		return nil, err
	}
	if this.basicAuth {
		request.SetBasicAuth(this.basicAuthUsername, this.basicAuthPassword)
	}
	if requestBody != nil {
		err = request.SetBody(requestBody)
		if err != nil {
			return nil, err
		}
	}

	res, err := this.c.Do((*http.Request)(request).WithContext(ctx))
	if err != nil {
		return nil, err
	}

	if res.Body != nil {
		defer res.Body.Close()
	}
	if err := checkResponse((*http.Request)(request), res); err != nil {
		response, err := this.newResponse(res)
		return response, err
	}
	return this.newResponse(res)
}

func (this *Client) Search(indexs ...string) *Query {
	return NewQuery(this).Index(indexs...)
}
