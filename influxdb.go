package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

// InfluxDBSink defines the data to allow us talk to an InfluxDB database
type InfluxDBSink struct {
	cluster  string
	c        client.Client
	bpConfig client.BatchPointsConfig
}

// types for the decoded fields and tags
type ptFields map[string]interface{}
type ptTags map[string]string

// GetInfluxDBWriter returns an InfluxDB DBWriter
func GetInfluxDBWriter() DBWriter {
	return &InfluxDBSink{}
}

// Init initializes an InfluxDBSink so that points can be written
// The array of argument strings comprises host, port, database
func (s *InfluxDBSink) Init(cluster string, args []string) error {
	var username, password string
	authenticated := false
	// args are host, port, database, and, optionally, username and password
	switch len(args) {
	case 3:
		authenticated = false
	case 5:
		authenticated = true
	default:
		return fmt.Errorf("InfluxDB Init() wrong number of args %d - expected 3", len(args))
	}

	s.cluster = cluster
	host, port, database := args[0], args[1], args[2]
	if authenticated {
		username = args[3]
		password = args[4]
	}
	url := "http://" + host + ":" + port

	s.bpConfig = client.BatchPointsConfig{
		Database:  database,
		Precision: "s",
	}

	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     url,
		Username: username,
		Password: password,
	})
	if err != nil {
		return fmt.Errorf("failed to create InfluxDB client - %v", err.Error())
	}
	s.c = c
	return nil
}

// WriteStats takes an array of StatResults and writes them to InfluxDB
func (s *InfluxDBSink) WriteCSStats(csstats []clientSummaryResult) error {
	log.Infof("WriteCSStats called for %d points", len(csstats))

	bp, err := client.NewBatchPoints(s.bpConfig)
	if err != nil {
		return fmt.Errorf("unable to create InfluxDB batch points - %v", err.Error())
	}
	for _, csstat := range csstats {
		fields := s.FieldsForCSstat(csstat)
		log.Debugf("got fields: %+v\n", fields)

		tags := s.TagsForCSStat(csstat)

		log.Debugf("got tags: %+v\n", tags)

		var pt *client.Point
		pt, err = client.NewPoint(csKeyName, tags, fields, time.Unix(csstat.Unixtime, 0).UTC())
		if err != nil {
			log.Warningf("failed to create point %q", csKeyName)
			continue
		}
		bp.AddPoint(pt)
	}
	// write the batch
	log.Infof("Writing %d points to InfluxDB", len(bp.Points()))
	log.Debugf("Points to be written: %+v\n", bp.Points())

	err = s.c.Write(bp)
	if err != nil {
		return fmt.Errorf("failed to write batch of points - %v", err.Error())
	}
	return nil
}

func (s *InfluxDBSink) FieldsForCSstat(csstat clientSummaryResult) ptFields {
	fields := make(ptFields)

	// Required fields
	fields["in_rate"] = csstat.In_rate
	fields["in_avg"] = csstat.In_avg
	fields["in_max"] = csstat.In_max
	fields["in_min"] = csstat.In_min
	fields["num_ops"] = csstat.Num_operations
	fields["ops_rate"] = csstat.Out_rate
	fields["out_avg"] = csstat.Out_avg
	fields["out_max"] = csstat.Out_max
	fields["out_min"] = csstat.Out_min
	fields["time_avg"] = csstat.Time_avg
	fields["time_max"] = csstat.Time_max
	fields["time_min"] = csstat.Time_min

	return fields
}

func (s *InfluxDBSink) TagsForCSStat(csstat clientSummaryResult) ptTags {
	tags := make(ptTags)
	// Required Tags
	tags["cluster"] = s.cluster
	tags["node"] = strconv.Itoa(csstat.Node)

	tags["local_address"] = *csstat.Local_addr
	tags["local_name"] = *csstat.Local_name
	tags["remote_address"] = *csstat.Remote_addr
	tags["remote_name"] = *csstat.Remote_name
	tags["class"] = *csstat.Class
	tags["protocol"] = *csstat.Protocol

	// Optional User Tags
	if csstat.User.Name != nil {
		tags["username"] = *csstat.User.Name
	}
	if csstat.User.Type != nil {
		tags["usertype"] = *csstat.User.Type
	}
	if csstat.User.Uid != nil {
		tags["user_id"] = *csstat.User.Uid
	}

	return tags
}

// helper function
func ptmapCopy(tags ptTags) ptTags {
	copy := ptTags{}
	for k, v := range tags {
		copy[k] = v
	}
	return copy
}
