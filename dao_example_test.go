package dao

/**
observation, "//Output:" works well but not "//Output :"
custom output from logging framework doesn't work
**/
func ExampleBuildMetricBucketBase() {
	ts := NewTS()
	metric := "metric-test-24"
	top := "2015-08-03 08:00:00 -0600 MDT"
	mid := "aekkk1234ffeeeeee"
	ts.BuildMetricBucketBase(metric, top, mid, 1, 1)

	////Output:
	//created an hour bucket for metric metric-test-24 topofThehour 2015-08-03 08:00:00 -0600 MDT

}
