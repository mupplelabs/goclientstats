package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	logging "github.com/op/go-logging"
)

// Version is the released program version
const Version = "0.01"
const userAgent = "goclientstats/" + Version

const (
	authtypeBasic   = "basic-auth"
	authtypeSession = "session"
)
const defaultAuthType = authtypeSession

var log = logging.MustGetLogger("goclientstats")

type loglevel logging.Level

var logFileName = flag.String("logfile", "./goclientstats.log", "pathname of log file")
var logLevel = loglevel(logging.NOTICE)
var configFileName = flag.String("config-file", "goclientstats.toml", "pathname of config file")

func (l *loglevel) String() string {
	level := logging.Level(*l)
	return level.String()
}

func (l *loglevel) Set(value string) error {
	level, err := logging.LogLevel(value)
	if err != nil {
		return err
	}
	*l = loglevel(level)
	return nil
}

func init() {
	// tie log-level variable into flag parsing
	flag.Var(&logLevel,
		"loglevel",
		"default log level [CRITICAL|ERROR|WARNING|NOTICE|INFO|DEBUG]")
}

func setupLogging() {
	f, err := os.OpenFile(*logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "goclientstats: unable to open log file %s for output - %s", *logFileName, err)
		os.Exit(2)
	}
	backend := logging.NewLogBackend(f, "", 0)
	var format = logging.MustStringFormatter(
		`%{time:2006-01-02T15:04:05Z07:00} %{shortfile} %{level} %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	backendLeveled := logging.AddModuleLevel(backendFormatter)
	backendLeveled.SetLevel(logging.Level(logLevel), "")
	logging.SetBackend(backendLeveled)
}

func main() {
	// parse command line
	flag.Parse()

	// set up logging
	setupLogging()

	// announce ourselves
	log.Noticef("Starting goclientstats version %s", Version)

	// read in our config
	conf := mustReadConfig()
	log.Info("Successfully read config file")

	// start collecting from each defined and enabled cluster
	var wg sync.WaitGroup
	for _, cl := range conf.Clusters {
		if cl.Disabled {
			log.Infof("skipping disabled cluster %q", cl.Hostname)
			continue
		}
		wg.Add(1)
		go func(cl clusterConf) {
			log.Infof("spawning collection loop for cluster %s", cl.Hostname)
			defer wg.Done()
			statsloop(cl, conf.Global)
			log.Infof("collection loop for cluster %s ended", cl.Hostname)
		}(cl)
	}
	wg.Wait()
	log.Notice("All collectors complete - exiting")
}

func statsloop(cluster clusterConf, gc globalConfig) {
	var err error
	var ss DBWriter

	// Connect to the cluster
	authtype := cluster.AuthType
	if authtype == "" {
		log.Infof("No authentication type defined for cluster %s, defaulting to %s", cluster.Hostname, authtypeSession)
		authtype = defaultAuthType
	}
	if authtype != authtypeSession && authtype != authtypeBasic {
		log.Warningf("Invalid authentication type %q for cluster %s, using default of %s", authtype, cluster.Hostname, authtypeSession)
		authtype = defaultAuthType
	}
	c := &Cluster{
		AuthInfo: AuthInfo{
			Username: cluster.Username,
			Password: cluster.Password,
		},
		AuthType:   authtype,
		Hostname:   cluster.Hostname,
		Port:       8080,
		VerifySSL:  cluster.SSLCheck,
		maxRetries: gc.maxRetries,
		ProtoList:  gc.ProtoList,
	}
	if err = c.Connect(); err != nil {
		log.Errorf("Connection to cluster %s failed: %v", c.Hostname, err)
		return
	}
	log.Infof("Connected to cluster %s, version %s", c.ClusterName, c.OSVersion)

	// Configure/initialize backend database writer
	ss, err = getDBWriter(gc.Processor)
	if err != nil {
		log.Error(err)
		return
	}
	err = ss.Init(c.ClusterName, gc.ProcessorArgs)
	if err != nil {
		log.Errorf("Unable to initialize %s plugin: %v", gc.Processor, err)
		return
	}

	// loop collecting and pushing stats
	log.Infof("Starting stat collection loop for cluster %s", c.ClusterName)

	for {
		//default update interval is 30s, set per MinUpdateInvtl in global config toml.
		curTime := time.Now()
		nextTime := curTime.Add(time.Second * time.Duration(gc.MinUpdateInvtl))

		// Collect one set of stats
		log.Infof("Cluster %s start collecting client summary stats", c.ClusterName)
		var sr []clientSummaryResult
		readFailCount := 0
		const maxRetryTime = time.Second * 1280
		retryTime := time.Second * 10
		for {
			sr, err = c.GetClientStats()
			if err == nil {
				break
			}
			readFailCount++
			log.Errorf("Failed to retrieve client summary stats for cluster %q: %v - retry #%d in %v", c.ClusterName, err, readFailCount, retryTime)
			time.Sleep(retryTime)
			if retryTime < maxRetryTime {
				retryTime *= 2
			}
		}

		log.Infof("Got %d client summary entries", len(sr))
		log.Infof("Cluster %s start writing stats to back end", c.ClusterName)

		// write stats, with retries
		for i := 0; i < gc.ProcessorMaxRetries; i++ {
			err = ss.WriteCSStats(sr)
			if err == nil {
				break
			}
			// Sleep for 60s
			time.Sleep(60 * time.Second)
		}

		if err != nil {
			log.Errorf("Failed to write stats to database: %s", err)
			return
		}

		curTime = time.Now()
		if curTime.Before(nextTime) {
			time.Sleep(nextTime.Sub(curTime))
		}
	}
}

// return a DBWriter for the given backend name
func getDBWriter(sp string) (DBWriter, error) {
	switch sp {
	case "influxdb_plugin":
		return GetInfluxDBWriter(), nil
	case "discard_plugin":
		return GetDiscardWriter(), nil
	default:
		return nil, fmt.Errorf("unsupported backend plugin %q", sp)
	}
}
