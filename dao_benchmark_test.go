package dao

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.comcast.com/viper-cog/clog"
)

// Unit Testing vs BenchMark testing semantics
// traditional time before and after and run with data
// go documentation
// how to do test preparation and teardown
// go test -bench Bench* -benchtime 1s
// benchmark tests with time cap
// benchmark tests on smaller functions
// benchmark parallel runs
// benchmark test re-use
// benchmark don't
// bench mark test time

/**
command: go test -bench ConvertByteToMap -run XXX -benchtime 1s
result : BenchmarkConvertByteToMap  300000   4031 ns/op
*/
func BenchmarkConvertByteToMap(b *testing.B) {
	//, metric-test-24, 55bf7c335e9518552d96dc3f
	reading := SampleSingleReading()
	for n := 0; n < b.N; n++ {
		ConvertByteToMap(reading)
	}

}

/**
command: go test -bench BuildMetricBucketBase -run XXX -benchtime 1s
result : BenchmarkBuildMetricBucketBase   1000000              1275 ns/op
*/
func BenchmarkBuildMetricBucketBase(b *testing.B) {
	ts := NewTS()
	metric := "metric-test-24"
	top := "2015-08-03 08:00:00 -0600 MDT"
	mid := "aekkk1234ffeeeeee"
	for n := 0; n < b.N; n++ {
		ts.BuildMetricBucketBase(metric, top, mid, 1, 1)
	}

}

/**
command: go test -bench Hash -run XXX -benchtime 1s
result : BenchmarkHash    2000000   792 ns/op
*/
func BenchmarkHash(b *testing.B) {
	reading := SampleSingleReading()
	mongo, _ := NewMongoHandler("localhost:27017", "sde", "md", "ts")
	for n := 0; n < b.N; n++ {
		mongo.Hash(reading)
	}

}

/**
command: go test -bench NewMongoHandler -run XXX -benchtime 1s
result: super fast
**/
func benchmarkNewMongoHandler(number int, b *testing.B) {
	//atomic operation
	for n := 0; n < b.N; n++ {
		NewMongoHandler("localhost:27017", "sde", "md", "ts")
	}

}

/**
command:	 go test -bench WriteTimeseries1000 -run XXX -benchtime 1m
result - all new metrics:  200         338443561 ns/op
result - existing metrics:  300         317823859 ns/op
**/
func BenchmarkWriteTimeseries50(b *testing.B)   { benchmarkWriteTimeseries(50, b) }
func BenchmarkWriteTimeseries100(b *testing.B)  { benchmarkWriteTimeseries(100, b) }
func BenchmarkWriteTimeseries500(b *testing.B)  { benchmarkWriteTimeseries(500, b) }
func BenchmarkWriteTimeseries1000(b *testing.B) { benchmarkWriteTimeseries(1000, b) }

func benchmarkWriteTimeseries(number int, b *testing.B) {
	// Initializing Mongo connection
	updates := make([][]byte, number)
	for i := 0; i < number; i++ {
		updates[i] = SampleSingleReading()
	}
	mongo, err := NewMongoHandler("localhost:27017", "sde", "md", "ts")
	//start a parallel update operation
	for n := 0; n < b.N; n++ {
		if err != nil {
			b.Fatal("didn't initialize mongo")
		}
		mongo.WriteTimeseries(updates)
	}
	//mongo.Destroy()

}
func SampleSingleReading() []byte {
	top := time.Now().Truncate(time.Hour).String()
	dataFeed := make(map[string]interface{})
	dataFeed["top"] = top
	dataFeed["metric"] = fmt.Sprint("metric-test-", strconv.Itoa(rand.Intn(5)))
	dataFeed["metadataId"] = fmt.Sprint("aerf", rand.Intn(2), "hkjf")
	dataFeed["m"] = strconv.Itoa(rand.Intn(60))
	dataFeed["s"] = strconv.Itoa(rand.Intn(12))
	dataFeed["value"] = rand.Intn(100000)
	b1, errb1 := json.Marshal(dataFeed)
	if errb1 != nil {
		clog.Error("Marshalling of datafeed failed.")
	}
	return b1
}