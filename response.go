package go_elasticsearch

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
)

// Response represents a response from Elasticsearch.
type Response struct {
	// StatusCode is the HTTP status code, e.g. 200.
	StatusCode int
	// Header is the HTTP header from the HTTP response.
	// Keys in the map are canonicalized (see http.CanonicalHeaderKey).
	Header http.Header
	// Body is the deserialized response body.
	Body json.RawMessage
}

func (this *Client) newResponse(res *http.Response) (*Response, error) {
	r := &Response{
		StatusCode: res.StatusCode,
		Header:     res.Header,
	}
	if res.Body != nil {
		data, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}
		if len(data) > 0 {
			r.Body = json.RawMessage(data)
		}
	}
	return r, nil
}
