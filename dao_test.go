package dao

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"
)

func TestPrepare(t *testing.T) {
	var updaterVal bson.M
	var updaterMetric bson.M
	// Initializing Mongo connection
	mongo, _ := NewMongoHandler("localhost:27017", "sde", "md", "ts")
	var mid string
	top := time.Now().Truncate(time.Hour).String()
	//create two distinct time series
	mid = "aerf4580900994fc"
	dataFeed := make(map[string]interface{})
	dataFeed["top"] = top
	dataFeed["metric"] = "test"
	dataFeed["metadataId"] = mid
	dataFeed["m"] = "1"
	dataFeed["s"] = "2"
	dataFeed["value"] = 4444444
	selector, updater := mongo.Prepare(dataFeed)
	if selector["top"] != top || selector["metric"] != "test" || selector["metadataId"] != mid {
		t.Fatal("selecter properly not prepared")
	}
	if updater["$set"].(bson.M) == nil {
		t.Fatal("updater properly not prepared")
	}
	updaterVal = updater["$set"].(bson.M)
	updaterMetric = updaterVal["m.1.s.2"].(bson.M)
	if updaterMetric["value"] == 4444444 {
		fmt.Println("Successfully prepared metric value data for batch")
	}
	if updaterMetric["value"] != 4444444 {
		t.Fatal("metric value not set properly")
	}

}

func TestWriteTimeSeries(t *testing.T) {
	t.Skip("skipping integration test. this test needs mongdb running")
	// Initializing Mongo connection
	mongo, err := NewMongoHandler("localhost:27017", "sde", "md", "ts")
	var mid string
	updates := make([][]byte, 2)
	if err != nil {
		t.Fatal("didn't initialize mongo")
	}
	_, e := mongo.tsc.Count()
	if e != nil {
		t.Fatal("failed to execute count", e)
	}
	top := time.Now().Truncate(time.Hour).String()
	//create two distinct time series
	mid = "aerf4580900994fc"
	dataFeed := make(map[string]interface{})
	dataFeed["top"] = top
	dataFeed["metric"] = "test"
	dataFeed["metadataId"] = mid
	dataFeed["m"] = "1"
	dataFeed["s"] = "2"
	dataFeed["value"] = 4444444
	b1, errb1 := json.Marshal(dataFeed)
	if errb1 != nil {
		t.Fatal("Marshalling of datafeed failed.")
	}
	updates[0] = b1
	dataFeed1 := make(map[string]interface{})
	dataFeed1["top"] = top
	dataFeed1["metric"] = "test1"
	dataFeed1["metadataId"] = mid
	dataFeed1["m"] = "2"
	dataFeed1["s"] = "4"
	dataFeed1["value"] = 888888888

	b2, errb2 := json.Marshal(dataFeed1)
	if errb2 != nil {
		t.Fatal("Marshalling of datafeed failed.")
	}
	updates[1] = b2
	err1 := mongo.WriteTimeseries(updates)
	if err1 != nil {
		t.Fatal("failed to update data in mongo")
	}

	//update existing
	updateExisting := make([][]byte, 1)
	existing := make(map[string]interface{})
	existing["top"] = top
	existing["metric"] = "test"
	existing["metadataId"] = mid
	existing["m"] = "33"
	existing["s"] = "31"
	existing["value"] = 161616161
	b3, errb3 := json.Marshal(existing)
	updateExisting[0] = b3
	if errb3 != nil {
		t.Fatal("Marshalling of datafeed failed.")
	}
	err2 := mongo.WriteTimeseries(updateExisting)
	if err2 != nil {
		t.Fatal("failed to update existing data in mongo")
	}

}

// // Validates that the packet.Copy method returns a new copy of the key:value
// // data structure.
func TestBuildMetricBucketBase(t *testing.T) {
	oneSec := Sec{123123}
	_, err := json.Marshal(oneSec)
	if err != nil {
		t.Fatal("Marshalling of sec failed.")
	}
	//os.Stdout.Write(s)
	oneMin := Min{}
	secondMap := make(map[string]Sec)
	secondMap["0"] = oneSec
	oneMin.Seconds = secondMap
	_, err1 := json.Marshal(oneMin)
	if err1 != nil {
		t.Fatal("Marshalling of Min failed.")
	}
	//os.Stdout.Write(s1)
	//var output TS
	mid := "fe090efeeeeee9900"
	ts := NewTS()
	ts.BuildMetricBucketBase("memory", time.Now().Truncate(time.Hour).String(), mid, 3, 3)
	if len(ts.Mins) != 3 {
		t.Fatal("Marshalling Time Series failed.")
	}

	packetBytes, err := json.Marshal(ts)
	if err != nil {
		t.Fatal("Marshalling Time Series failed.")
	}
	if len(packetBytes) == 0 {
		t.Fatal("Marshalling Timeseries failed.")
	}
	l := ioutil.WriteFile("/tmp/ts.json", packetBytes, 0666)
	if l != nil {

	}
}
