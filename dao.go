package dao

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	debug             bool
	mongoAddr         string
	mongoMdCollection string
	mongoTsCollection string
	mongoDB           string
)

const (
	MINS    int    = 1
	SECONDS int    = 1
	TOP     string = "top"
	METRIC  string = "metric"
	MIN     string = "m"
	SEC     string = "s"
	METID   string = "metadataId"
	VALUE   string = "value"
	OBJID   string = "_id"
)

type MongoHandler struct {
	s   *mgo.Session
	md  *mgo.Collection
	tsc *mgo.Collection
	db  *mgo.Database
}

type TS struct {
	Top        string         `json:"top" bson:"top"`
	Metric     string         `json:"metric" bson:"metric"`
	MetadataId string         `json:"metadataId" bson:"metadataId"`
	Mins       map[string]Min `json:"m" bson:"m"`
}

type Min struct {
	Seconds map[string]Sec `json:"s" bson:"s"`
}

type Sec struct {
	Value float64 `json:"value" bson:"value"`
}

func NewTS() *TS {
	return &TS{}
}

func init() {
	flag.BoolVar(&debug, "debug", false, "using default flag")
	flag.Parse()
}

func NewMongoHandler(maddr string, mdb string, mdcname string, mtsname string) (*MongoHandler, error) {
	if debug == true {
		log.Printf("mongodb address", maddr, mdb, mdcname, mtsname)
	}
	m := &MongoHandler{}
	session, err := mgo.DialWithTimeout(maddr, 20*time.Millisecond)
	if err != nil {
		return m, err
	}
	m.s = session
	m.db = session.DB(mdb)
	m.md = m.db.C(mdcname)
	m.tsc = m.db.C(mtsname)
	return m, nil
}

func (m *MongoHandler) Destroy() {
	m.s.Close()
}

func (m *MongoHandler) WriteTimeseries(messages [][]byte) error {
	//clog.Infof("Writing %d timeseries messages to Mongo", len(messages))
	var batch []bson.M
	for _, message := range messages {
		in, err := ConvertByteToMap(message)
		if err != nil {
			log.Fatalf("Error transforming collectd data ")
			continue
		}
		selector, updater := m.Prepare(in)
		if nil == selector || nil == updater {
			continue
		}
		one := bson.M{"q": selector, "u": updater, "multi": false, "upsert": true}
		if debug == true {
			log.Printf("appended update to bulk", one)
		}
		batch = append(batch, one)
	}
	if len(batch) > 0 {
		var result interface{}
		command := bson.M{"update": "ts", "updates": batch, "writeConcern": bson.M{"w": 0, "j": false, "wtimeout": 1000}, "ordered": false}
		if debug == true {
			log.Printf("command:", command)
		}
		//these are panics
		if m == nil {
			log.Fatal("nil mongo session not allowed, mongodb is a pre-req")
		}
		if m.db == nil {
			log.Fatal("nil mongo database not allowed, mongodb is a pre-req")
		}
		if command == nil {
			log.Fatal("nil command not allowed, mongodb is a pre-req")
		}
		m.db.Run(command, &result)
		if debug == true {
			log.Printf("update result", result)
		}
	}
	return nil
}

// create a hash on the metadata.
func (m *MongoHandler) Hash(p []byte) string {
	hasher := md5.New()
	hasher.Write(p)
	return hex.EncodeToString(hasher.Sum(nil))
}

func (m *MongoHandler) Prepare(in map[string]interface{}) (bson.M, bson.M) {
	var updatekey string
	var selector bson.M
	var updater bson.M
	// there should be unique for a metric
	if nil != in {
		top := in[TOP].(string)
		metric := in[METRIC].(string)
		mkey := in[MIN].(string)
		skey := in[SEC].(string)
		mid := in[METID].(string)
		if len(top) == 0 || len(metric) == 0 || len(skey) == 0 || len(mkey) == 0 {
			log.Fatalf("Invalid top %s metric %s mkey %s skey %s", top, metric, mkey, skey)
			//return errors.New("Invalid data")
		}
		// build a document
		updatekey = fmt.Sprintf("m.%s.s.%s", mkey, skey)
		if debug == true {
			log.Print(updatekey)
		}
		selector = bson.M{TOP: top, METRIC: metric, METID: mid}
		if debug == true {
			log.Print(selector)
		}
		updater = bson.M{"$set": bson.M{updatekey: bson.M{VALUE: in[VALUE]}}}
		return selector, updater
	}
	return nil, nil
}

func (ts *TS) BuildMetricBucketBase(metric string, topOfTheHour string, mid string, minsConst int, secsConst int) *TS {
	//map with top of the hour as key and various MetricBuckets [metric, mins]
	mins := make(map[string]Min)
	for m := 1; m <= minsConst; m++ {
		seconds := make(map[string]Sec)
		for s := 1; s <= secsConst; s++ {
			sec := Sec{}
			seconds[strconv.Itoa(s)] = sec
		}
		min := Min{Seconds: seconds}
		mins[strconv.Itoa(m)] = min
	}
	ts.Mins = mins
	ts.Metric = metric
	ts.Top = topOfTheHour
	ts.MetadataId = mid
	if debug == true {
		log.Print("created an hour bucket for metric %s topofThehour %s", metric, topOfTheHour)
	}
	//fmt.Printf("created an hour bucket for metric %s topofThehour %s", metric, topOfTheHour)
	return ts
}

//good one
// func ConvertByteToMap(msg []byte) (map[string]interface{}, error) {
// 	var f interface{}
// 	err := json.Unmarshal(msg, &f)
// 	out := f.(map[string]interface{})
// 	return out, err
// }

//slower
func ConvertByteToMap(msg []byte) (map[string]interface{}, error) {
	f := make(map[string]interface{})
	err := json.Unmarshal(msg, &f)
	return f, err
}
