package go_elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type ConditionFunc func(operator string, operands []interface{}) (map[string]interface{}, error)

type QueryBuilder struct{}

func (this *QueryBuilder)Build(query *Query) (map[string]interface{}, error) {
	parts := make(map[string]interface{})

	if query.limit > 0{
		parts["size"] = query.limit
	}
	if query.offset > 0{
		parts["from"] = query.offset
	}
	if query.explain == true{
		parts["explain"] = query.explain
	}
	whereQuery ,_ := this.BuildCondition(query.where)
	if whereQuery != nil{
		parts["query"] = whereQuery
	}else if query.query != nil{
		parts["query"] = query.query
	}

	if query.orderBy != nil{
		parts["sort"] = query.orderBy
	}

	if query.aggregations != nil{
		parts["aggregations"] = query.aggregations
	}
	return parts,nil
}

func (this *QueryBuilder) BuildCondition(condition []interface{}) (interface{}, error) {
	builders := map[string]ConditionFunc{
		"not":         buildNotCondition,
		"and":         buildBoolCondition,
		"or":          buildBoolCondition,
		"between":     buildBetweenCondition,
		"not between": buildBetweenCondition,
		"in":          buildInCondition,
		"not in":      buildInCondition,
		"like":        buildLikeCondition,
		"not like":    buildLikeCondition,
		"or like":     buildLikeCondition,
		"or not like": buildLikeCondition,
		"lt":          buildHalfBoundedRangeCondition,
		"<":           buildHalfBoundedRangeCondition,
		"lte":         buildHalfBoundedRangeCondition,
		"<=":          buildHalfBoundedRangeCondition,
		"gt":          buildHalfBoundedRangeCondition,
		">":           buildHalfBoundedRangeCondition,
		"gte":         buildHalfBoundedRangeCondition,
		">=":          buildHalfBoundedRangeCondition,
	}

	if len(condition) >= 1 {
		operator, ok := condition[0].(string)
		if !ok {
			ret, err := buildHashCondition(condition)
			return ret, err
		}
		operator = strings.ToLower(operator)
		if method, ok := builders[operator]; ok {
			condition = condition[1:]
			ret, err := method(operator, condition)
			return ret, err
		}
	}
	return nil, nil
}

func buildQueryFromWhere(condition []interface{}) (map[string]interface{}, error) {
	builder := QueryBuilder{}
	buildCondition, e := builder.BuildCondition(condition)
	if e != nil || buildCondition != nil{
		return nil,e
	}

	//query := map[string]interface{}{"query":map[string]interface{}{"constant_score": map[string]interface{}{"filter": buildCondition}}}
	query := map[string]interface{}{"constant_score": map[string]interface{}{"filter": buildCondition}}
	return query, e
}

func main() {
	//Array
	//(
	//    [0] => and
	//    [1] => Array
	//        (
	//            [0] => between
	//            [1] => F_OrderTime
	//            [2] => 2020-03-07T00:00:00
	//            [3] => 2020-03-07T23:59:59
	//        )
	//
	//    [2] => Array
	//        (
	//            [0] => and
	//            [1] => Array
	//                (
	//                    [F_FJScan_Flag] => 1
	//                )
	//
	//        )
	//
	//    [3] => Array
	//        (
	//            [0] => or
	//            [1] => Array
	//                (
	//                    [F_SJ_ScanFlag] => 0
	//                )
	//
	//            [2] => Array
	//                (
	//                    [0] => IN
	//                    [1] => F_SJ_ScanFlag
	//                    [2] => Array
	//                        (
	//                            [0] =>
	//                        )
	//
	//                )
	//
	//        )
	//
	//    [4] => Array
	//        (
	//        )
	//
	//)
	condition := make([]interface{}, 5, 5)
	condition[0] = "and"
	condition[1] = []interface{}{"between", "F_OrderTime", "2020-03-07T00:00:00", "2020-03-07T23:59:59"}
	condition[2] = []interface{}{"and", []interface{}{map[string]interface{}{"F_FJScan_Flag": []string{"1"}}}} //map[string]interface{}{"F_FJScan_Flag": "1"}
	condition[3] = []interface{}{"or", []interface{}{map[string]interface{}{"F_SJ_ScanFlag": "0"}}, []interface{}{"IN", "F_SJ_ScanFlag", "null"}}
	condition[4] = []interface{}{}
	query, _ := buildQueryFromWhere(condition)
	bytes, e := json.Marshal(query)
	fmt.Println(string(bytes), e)
}

func buildHashCondition(condition []interface{}) (map[string]interface{}, error) {
	var (
		parts       = make([]interface{}, 0)
		emptyFields = make([]interface{}, 0)
		query       = make(map[string]interface{}, 0)
	)

	for _, values := range condition {
		if v,ok := values.(map[string]interface{});ok{
			for attribute, value := range v {
				if attribute == "_id" {
					if value == "null" {
						parts = append(parts, map[string]interface{}{"terms": map[string]interface{}{"_uid": nil}})
					} else {
						//TODO  $parts[] = ['ids' => ['values' => is_array($value) ? $value : [$value]]];
					}
				} else {
					switch t := value.(type) {
					case []string:
						if len(t) > 0 {
							parts = append(parts, map[string]interface{}{"terms": map[string]interface{}{attribute: value}})
						}
					case string:
						if value == "null" {
							emptyFields = append(emptyFields, map[string]interface{}{"exists": map[string]interface{}{"field": attribute}})
						} else {
							parts = append(parts, map[string]interface{}{"term": map[string]interface{}{attribute: value}})
						}
					}
				}
			}
		}
	}
	query["must"] = parts
	if len(emptyFields) > 0 {
		query["must_not"] = emptyFields
	}
	return map[string]interface{}{"bool": query}, nil
}

func buildNotCondition(operator string, operands []interface{}) (map[string]interface{}, error) {
	return nil, nil
}
func buildBoolCondition(operator string, operands []interface{}) (map[string]interface{}, error) {
	var (
		clause       string
		parts        = make([]interface{}, 0)
		queryBuilder = QueryBuilder{}
	)

	if operator == "and" {
		clause = "must"
	} else if operator == "or" {
		clause = "should"
	} else {
		return nil, errors.New("Operator should be or' or 'and'")
	}
	for _, operand := range operands {
		switch t := operand.(type) {
		case []interface{}:
			operand, _ = queryBuilder.BuildCondition(t)
		}

		if operand != nil {
			parts = append(parts, operand)
		}
	}
	if len(parts) > 0 {
		return map[string]interface{}{"bool": map[string]interface{}{clause: parts}}, nil
	}
	return nil, nil
}
func buildBetweenCondition(operator string, operands []interface{}) (map[string]interface{}, error) {
	var (
		column string
		value1 string
		value2 string
		filter map[string]interface{}
	)
	if len(operands) != 3 {
		return nil, errors.New("Operator " + operator + " requires three operands.")
	}
	column, value1, value2 = operands[0].(string), operands[1].(string), operands[2].(string)
	filter = map[string]interface{}{"range": map[string]interface{}{column: map[string]string{"gte": value1, "lte": value2}}}
	if operator == "not between" {
		filter = map[string]interface{}{"bool": map[string]interface{}{"must_not": filter}}
	}
	return filter, nil
}
func buildInCondition(operator string, operands []interface{}) (map[string]interface{}, error) {
	var (
		canBeNull bool
		column    string
		filter    map[string]interface{}
	)
	//fmt.Println(operands)
	if len(operands) != 2 {
		return nil, errors.New("Operator " + operator + " requires two operands.")
	}
	column, ok := operands[0].(string)
	if !ok {
		return nil, errors.New(fmt.Sprintf("buildInCondition column %v assert error.", operands[0]))
	}
	switch t := operands[1].(type) {
	case []string:
		if len(t) > 0 {
			for k, v := range t {
				if v == "null" {
					canBeNull = true
					t = append(t[:k], t[k+1:]...)
				}
			}
			if column == "_id" {
				filter = map[string]interface{}{"ids": map[string]interface{}{"values": t}}
				if canBeNull == true {
					filter = map[string]interface{}{"bool": map[string]interface{}{"should": filter, "bool": map[string]interface{}{"must_not": map[string]interface{}{"exists": map[string]string{"field": column}}}}}
				}
			} else {
				filter = map[string]interface{}{"terms": map[string]interface{}{column: t}}
				if canBeNull == true {
					filter = map[string]interface{}{"bool": map[string]interface{}{"should": filter, "bool": map[string]interface{}{"must_not": map[string]interface{}{"exists": map[string]string{"field": column}}}}}
				}
			}
		} else {
			if column == "_id" {
				if canBeNull == true { // there is no null pk
					// this condition is equal to WHERE false
					filter = map[string]interface{}{"terms": map[string]interface{}{"_uid": nil}}
				}
			} else {
				filter = map[string]interface{}{"bool": map[string]interface{}{"must_not": map[string]interface{}{"exists": map[string]string{"field": column}}}}
			}
		}
	case string:
		if t == "null" || t == "" {
			canBeNull = true

			if column == "_id" {
				if canBeNull == true { // there is no null pk
					// this condition is equal to WHERE false
					filter = map[string]interface{}{"term": map[string]interface{}{"_uid": nil}}
				}
			} else {
				filter = map[string]interface{}{"bool": map[string]interface{}{"must_not": map[string]interface{}{"exists": map[string]string{"field": column}}}}
			}
		} else {
			if column == "_id" {
				filter = map[string]interface{}{"id": map[string]interface{}{"value": t}}
				if canBeNull == true {
					filter = map[string]interface{}{"bool": map[string]interface{}{"should": filter, "bool": map[string]interface{}{"must_not": map[string]interface{}{"exists": map[string]string{"field": column}}}}}
				}
			} else {
				filter = map[string]interface{}{"term": map[string]interface{}{column: t}}
				if canBeNull == true {
					filter = map[string]interface{}{"bool": map[string]interface{}{"should": filter, "bool": map[string]interface{}{"must_not": map[string]interface{}{"exists": map[string]string{"field": column}}}}}
				}
			}
		}

	}
	if operator == "not in" {
		filter = map[string]interface{}{
			"bool": map[string]interface{}{"must_not": filter},
		}
	}
	return filter, nil
}
func buildLikeCondition(operator string, operands []interface{}) (map[string]interface{}, error) {
	return nil, errors.New("like conditions are not supported by elasticsearch.")
}

//Builds a half-bounded range condition
//(for "gt", ">", "gte", ">=", "lt", "<", "lte", "<=" operators)
func buildHalfBoundedRangeCondition(operator string, operands []interface{}) (map[string]interface{}, error) {
	var (
		column        string
		value         string
		rangeOperator string
		operatorKey   = map[string]string{
			"gte": "gte",
			">=":  "gte",
			"lte": "lte",
			"<=":  "lte",
			"gt":  "gt",
			">":   "gt",
			"lt":  "lt",
			"<":   "lt",
		}
	)
	if len(operands) != 2 {
		return nil, errors.New("Operator " + operator + " requires two operands.")
	}
	column, value = operands[0].(string), operands[1].(string)
	if column == "_id" {
		column = "_uid"
	}
	if v, ok := operatorKey[operator]; ok {
		rangeOperator = v
	}
	if rangeOperator == "" {
		return nil, errors.New("Operator " + operator + " is not implemented.")
	}
	return map[string]interface{}{"range": map[string]interface{}{column: map[string]string{rangeOperator: value}}}, nil
}
