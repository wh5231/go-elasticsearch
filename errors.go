package go_elasticsearch

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

func checkResponse(req *http.Request, res *http.Response) error {
	if res.StatusCode >= 200 && res.StatusCode < 300 {
		return nil
	}
	return createResponseError(res)
}
// createResponseError creates an Error structure from the HTTP response,
// its status code and the error information sent by Elasticsearch.
func createResponseError(r *http.Response) error {
	if r.Body != nil {
		return &Error{Status: r.StatusCode}
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return &Error{Status: r.StatusCode}
	}
	errReply := new(Error)
	err = json.Unmarshal(data, &errReply)
	if err != nil {
		return &Error{Status: r.StatusCode}
	}

	if errReply != nil {
		if errReply.Status == 0 {
			return &Error{Status: r.StatusCode}
		}
		return errReply
	}
	return &Error{Status: r.StatusCode}
}

// Error encapsulates error details as returned from Elasticsearch.
type Error struct {
	Status  int           `json:"status"`
	Details *ErrorDetails `json:"error,omitempty"`
}

// ErrorDetails encapsulate error details from Elasticsearch.
// It is used in e.g. elastic.Error and elastic.BulkResponseItem.
type ErrorDetails struct {
	Type         string                   `json:"type"`
	Reason       string                   `json:"reason"`
	ResourceType string                   `json:"resource.type,omitempty"`
	ResourceId   string                   `json:"resource.id,omitempty"`
	Index        string                   `json:"index,omitempty"`
	Phase        string                   `json:"phase,omitempty"`
	Grouped      bool                     `json:"grouped,omitempty"`
	CausedBy     map[string]interface{}   `json:"caused_by,omitempty"`
	RootCause    []*ErrorDetails          `json:"root_cause,omitempty"`
	FailedShards []map[string]interface{} `json:"failed_shards,omitempty"`
}

// Error returns a string representation of the error.
func (e *Error) Error() string {
	if e.Details != nil && e.Details.Reason != "" {
		return fmt.Sprintf("elastic: Error %d (%s): %s [type=%s]", e.Status, http.StatusText(e.Status), e.Details.Reason, e.Details.Type)
	}
	return fmt.Sprintf("elastic: Error %d (%s)", e.Status, http.StatusText(e.Status))
}
