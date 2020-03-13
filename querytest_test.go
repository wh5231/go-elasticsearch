package go_elasticsearch

import (
	"context"
	"fmt"
	"testing"
)

func TestQuery(t *testing.T)  {
	client,err := NewClient(SetUrl("http://es-cn-v641b8vch000cf5wy.public.elasticsearch.aliyuncs.com:9200"),SetBasicAuth("elastic", "aA13810253698"))
	fmt.Println("err",err)
	/*options := map[string]interface{}{
		"terms":map[string]interface{}{"field":"F_O_CustomerName.keyword","size":10},
		"aggregations":map[string]interface{}{
			"carriage":map[string]interface{}{"sum":map[string]string{"field":"F_Freight"}},
		},
	}*/
	/*where,_ := client.Search("md_fin_waybill").Type("md_fin_waybill").AndWhere("test","1").AndWhere("between", "F_OrderTime", "2020-03-07T00:00:00", "2020-03-07T23:59:59").AndWhere("in", "F_FJScan_Flag", "0").OrWhere("<", "F_FJScan_Flag", "1").OrWhere("in", "F_FJScan_Flag", "2").AddAggregate("group_by_customer_name",options).Do(context.Background())
*/


	where,_ := client.Search("md_fin_waybill").Type("md_fin_waybill").AndWhere("between", "F_OrderTime", "2020-03-07T00:00:00", "2020-03-07T23:59:59").OrderBy(map[string]string{"F_OrderTime":"asc"}).Do(context.Background())
	fmt.Println(where)
}
