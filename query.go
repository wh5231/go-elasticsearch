package go_elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/wh5231/go-elasticsearch/uritemplates"
	"net/url"
	"strings"
)

type Query struct {
	client       *Client
	scriptFields []string
	index        []string
	typ          []string
	timeout      string
	limit        int
	offset       int
	source       interface{}
	where        []interface{}
	query        []interface{}
	aggregations map[string]interface{}
	suggest      map[string]interface{}
	orderBy      []map[string]string
	//array options to be appended to the query URL, such as "search_type" for search or "timeout" for delete
	options map[string]string
	explain bool
}

func NewQuery(c *Client) *Query {
	return &Query{
		client:       c,
		limit:        10,
		offset:       0,
		aggregations: make(map[string]interface{}),
		suggest:      make(map[string]interface{}),
		orderBy:      make([]map[string]string, 0),
		options:      make(map[string]string),
	}
}

// Index sets the names of the indices to use for search.
func (this *Query) Index(index ...string) *Query {
	this.index = append(this.index, index...)
	return this
}
func (this *Query) Type(typ ...string) *Query {
	this.typ = append(this.typ, typ...)
	return this
}

func (this *Query) Limit(i int) *Query {
	this.limit = i
	return this
}

func (this *Query) Offset(i int) *Query {
	this.offset = i
	return this
}

func (this *Query) Source(source interface{}) *Query {
	this.source = source
	return this
}

func (this *Query) AndWhere(condition ...interface{}) *Query {
	if this.where == nil {
		this.where = []interface{}{"and", condition}
	} else if len(condition) == 2 {
		if column, ok := condition[0].(string); ok {
			this.where = append(this.where, "and", map[string]interface{}{column: condition[1]})
		}
	} else {
		this.where = append(this.where, []interface{}{"and", condition})
	}
	return this
}

//
func (this *Query) OrWhere(condition ...interface{}) *Query {
	if this.where == nil {
		this.where = []interface{}{"and", condition}
	} else if len(condition) == 2 {
		if column, ok := condition[0].(string); ok {
			this.where = append(this.where, "or", map[string]interface{}{column: condition[1]})
		}
	} else {
		this.where = append(this.where, []interface{}{"or", condition})
	}
	return this
}

func (this *Query) OrderBy(orderBy ...map[string]string) *Query {
	if this.orderBy == nil {
		this.orderBy = orderBy
	} else {
		this.orderBy = append(this.orderBy, orderBy...)
	}
	return this
}

//Query sets the query to perform, e.g. MatchAllQuery.
func (this *Query) Query(query ...interface{}) *Query {
	if this.query == nil {
		this.query = query
	} else {
		this.query = append(this.query, query)
	}
	return this
}

// Adds an aggregation to this query.
// param string name the name of the aggregation
// param string type the aggregation type. e.g. `terms`, `range`,
// `histogram`, ...
// param string|array $options the configuration options for this
func (this *Query) AddAgg(name, typ string, options map[string]interface{}) *Query {
	this.AddAggregate(name, map[string]interface{}{typ: options})
	return this
}

//Adds an aggregation to this query. Supports nested aggregations
//param string $name the name of the aggregation
//param string|array $options the configuration options for this
func (this *Query) AddAggregate(name string, options map[string]interface{}) *Query {
	this.aggregations[name] = options
	return this
}

func (this *Query) Timeout(timeout string) *Query {
	this.timeout = timeout
	return this
}

func (this *Query) Options(name, options string) *Query {
	this.options[name] = options
	return this
}
func (this *Query) Explain(explain bool) *Query {
	this.explain = explain
	return this
}

//buildURL builds the URL for the operation.
func (this *Query) BuildUrl() (string, url.Values, error) {
	var (
		err    error
		path   string
		params = url.Values{}
	)
	if len(this.typ) > 0 && len(this.index) > 0 {
		path, err = uritemplates.Expand("/{index}/{type}/_search", map[string]string{
			"index": strings.Join(this.index, ","),
			"type":  strings.Join(this.typ, ","),
		})
	} else if len(this.index) > 0 {
		path, err = uritemplates.Expand("/{index}/_search", map[string]string{
			"index": strings.Join(this.index, ","),
		})
	} else if len(this.typ) > 0 {
		path, err = uritemplates.Expand("/{type}/_search", map[string]string{
			"type": strings.Join(this.typ, ","),
		})
	} else {
		path = "/_search"
	}
	if err != nil {
		return "", url.Values{}, err
	}
	// Add query string parameters

	for k, v := range this.options {
		params.Add(k, v)
	}
	return path, params, nil
}

func (this *Query) Do(ctx context.Context) (interface{}, error) {
	builder := QueryBuilder{}
	path, values, err := this.BuildUrl()
	if err != nil {
		return nil, err
	}
	body, err := builder.Build(this)
	if err != nil {
		return nil, err
	}
	response, err := this.client.httpRequest(ctx, "GET", path, values, body, false)
	if err != nil {
		return nil, err
	}
	fmt.Println(string(response.Body))
	result := new(SearchResult)
	json.Unmarshal([]byte(response.Body), &result)
	return result, err
}

type SearchResult struct {
	Shards struct {
		Failed     int64 `json:"failed"`
		Successful int64 `json:"successful"`
		Total      int64 `json:"total"`
	} `json:"_shards"`
	Hits struct {
		Hits []struct {
			ID     string           `json:"_id"`
			Index  string           `json:"_index"`
			Score  int64            `json:"_score"`
			Source *json.RawMessage `json:"_source"`
			Type   string           `json:"_type"`
		} `json:"hits"`
		MaxScore int64 `json:"max_score"`
		Total    int64 `json:"total"`
	} `json:"hits"`
	TimedOut     bool             `json:"timed_out"`
	Aggregations *json.RawMessage `json:"aggregations,omitempty"` // results from aggregations
	Took         int64            `json:"took"`
}
