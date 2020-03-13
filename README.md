# go-elasticsearch
使用sql写es

//创建客户端

client,err := NewClient(SetUrl(""),SetBasicAuth("elastic", "elastic"))


//增加示例aggregations

options := map[string]interface{}{
	"terms":map[string]interface{}{"field":"F_O_CustomerName.keyword","size":10},
	"aggregations":map[string]interface{}{
		"carriage":map[string]interface{}{"sum":map[string]string{"field":"F_Freight"}},
	},
}

//添加条件

AndWhere("test","1")

AndWhere("between", "F_OrderTime", "2020-03-07T00:00:00", "2020-03-07T23:59:59")

where,_ := client.Search("index").Type("type").AndWhere("test","1").AndWhere("between", "F_OrderTime", "2020-03-07T00:00:00", "2020-03-07T23:59:59").AndWhere("in", "F_FJScan_Flag", "0").OrWhere("<", "F_FJScan_Flag", "1").OrWhere("in", "F_FJScan_Flag", "2").AddAggregate("group_by_customer_name",options).Do(context.Background())
