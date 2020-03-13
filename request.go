package go_elasticsearch

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

type Request http.Request

func NewRequest(method, path string) (*Request, error) {
	req, err := http.NewRequest(method, path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("User-Agent", "go_elasticsearch")
	req.Header.Add("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	return (*Request)(req), nil
}

func (this *Request)SetBasicAuth(username,password string)  {
	((*http.Request)(this)).SetBasicAuth(username,password)
}

//setBodyJson encodes the body as a struct to be marshaled via json.Marshal.
func (this *Request)SetBody(data interface{}) error {
	body, err := json.Marshal(data)
	if err != nil{
		return err
	}
	this.Header.Set("Content-Type", "application/json")
	this.setBodyReader(bytes.NewReader(body))
	return nil
}
// setBodyReader writes the body from an io.Reader.
func (this *Request)setBodyReader(body io.Reader) {
	rc,ok := body.(io.ReadCloser)
	if !ok && body != nil{
		rc =ioutil.NopCloser(body)
	}
	this.Body = rc
	if body != nil{
		switch v := body.(type) {
		case *strings.Reader:
			this.ContentLength = int64(v.Len())
		case *bytes.Buffer:
			this.ContentLength = int64(v.Len())
		case *bytes.Reader:
			this.ContentLength = int64(v.Len())
		}
	}

}