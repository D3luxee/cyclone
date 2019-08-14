/*-
 * Copyright © 2016,2017 Jörg Pernfuß <code.jpe@gmail.com>
 * Copyright © 2016, 1&1 Internet SE
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package main // import "github.com/solnx/cyclone/cmd/cyclone"

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	wall "github.com/solnx/eye/lib/eye.wall"

	"github.com/Sirupsen/logrus"
	"github.com/client9/reopen"
	"github.com/cyberdelia/go-metrics-graphite"
	"github.com/mjolnir42/delay"
	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/limit"
	"github.com/rcrowley/go-metrics"
	"github.com/solnx/cyclone/internal/cyclone"
)

var githash, shorthash, builddate, buildtime string

func init() {
	// Discard logspam from Zookeeper library
	erebos.DisableZKLogger()

	// set standard logger options
	erebos.SetLogrusOptions()

	// redirect go default logger to /dev/null
	log.SetOutput(ioutil.Discard)
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func main() {
	var (
		err         error
		configFlag  string
		logFH       *reopen.FileWriter
		versionFlag bool
	)
	flag.StringVar(&configFlag, `config`, `cyclone.conf`,
		`Configuration file location`)
	flag.BoolVar(&versionFlag, `version`, false,
		`Print version information`)
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
	// only provide version information if --version was specified
	if versionFlag {
		fmt.Fprintln(os.Stderr, `Cyclone Metric Monitoring System`)
		fmt.Fprintf(os.Stderr, "Version  : %s-%s\n", builddate,
			shorthash)
		fmt.Fprintf(os.Stderr, "Git Hash : %s\n", githash)
		fmt.Fprintf(os.Stderr, "Timestamp: %s\n", buildtime)
		os.Exit(0)
	}

	// read runtime configuration
	conf := erebos.Config{}
	if err = conf.FromFile(configFlag); err != nil {
		logrus.Fatalf("Could not open configuration: %s", err)
	}
	panicLog, err := os.OpenFile(filepath.Join(conf.Log.Path, `panic.log`), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0660)
	redirectStderr(panicLog)
	if err != nil {
		logrus.Fatal(err)
	}
	logger := logrus.New()
	// setup logfile
	if logFH, err = reopen.NewFileWriter(
		filepath.Join(conf.Log.Path, conf.Log.File),
	); err != nil {
		logrus.Fatalf("Unable to open logfile: %s", err)
	} else {
		conf.Log.FH = logFH
	}
	logger.SetOutput(conf.Log.FH)
	logger.Infoln(`Starting CYCLONE...`)

	// switch to requested loglevel
	// trace, debug, info, warning, error, fatal, panic
	switch strings.ToLower(conf.Log.LogLevel) {
	case `trace`:
		logger.SetLevel(logrus.TraceLevel)
	case `debug`:
		logger.SetLevel(logrus.DebugLevel)
	case `info`:
		logger.SetLevel(logrus.InfoLevel)
	case `warning`:
		logger.SetLevel(logrus.WarnLevel)
	case `error`:
		logger.SetLevel(logrus.ErrorLevel)
	case `fatal`:
		logger.SetLevel(logrus.FatalLevel)
	case `panic`:
		logger.SetLevel(logrus.PanicLevel)
	default:
		logger.SetLevel(logrus.InfoLevel)
	}
	// signal handler will reopen logfile on USR2 if requested
	if conf.Log.Rotate {
		sigChanLogRotate := make(chan os.Signal, 1)
		signal.Notify(sigChanLogRotate, syscall.SIGUSR2)
		go erebos.Logrotate(sigChanLogRotate, conf)
	}

	// setup signal receiver for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// this channel is used by the handlers on error
	handlerDeath := make(chan error)
	// this channel is used to signal the consumer to stop
	consumerShutdown := make(chan struct{})
	// this channel will be closed by the consumer
	consumerExit := make(chan struct{})

	// setup goroutine waiting policy
	waitdelay := delay.New()

	// setup metrics
	var metricPrefix string
	switch conf.Misc.InstanceName {
	case ``:
		metricPrefix = `cyclone`
	default:
		metricPrefix = fmt.Sprintf("cyclone.%s",
			conf.Misc.InstanceName)
	}
	pfxRegistry := metrics.NewPrefixedRegistry(metricPrefix)
	metrics.NewRegisteredMeter(`.metrics.consumed`,
		pfxRegistry)
	metrics.NewRegisteredMeter(`.metrics.discarded`,
		pfxRegistry)
	metrics.NewRegisteredMeter(`.metrics.processed`,
		pfxRegistry)
	metrics.NewRegisteredMeter(`.evaluations`,
		pfxRegistry)
	metrics.NewRegisteredMeter(`.alarms.sent`,
		pfxRegistry)
	metrics.NewRegisteredHistogram(`.alarm.delay.seconds`,
		pfxRegistry, metrics.NewExpDecaySample(1028, 0.03))
	metrics.GetOrRegisterGauge(`.alarmapi.error`,
		pfxRegistry).Update(0)

	if conf.Misc.ProduceMetrics {
		logger.Info(`Launched metrics producer socket`)
		addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%s", conf.Graphite.Host, conf.Graphite.Port))
		if err != nil {
			logger.Fatalln(err)
		}
		go graphite.Graphite(pfxRegistry, time.Duration(conf.Graphite.FlushInterval*time.Second.Nanoseconds()), conf.Graphite.Prefix, addr)
	}

	cyclone.AgeCutOff = time.Duration(
		conf.Cyclone.MetricsMaxAge,
	) * time.Minute * -1

	// acquire shared concurrency limit
	lim := limit.New(conf.Cyclone.ConcurrencyLimit)
	lookup := wall.NewLookup(&conf, `cyclone`)
	lookup.SetLogger(logger)
	if err := lookup.Start(); err != nil {
		logger.Fatalln(err)
	}
	defer lookup.Close()
	// start application handlers
	for i := 0; i < runtime.NumCPU(); i++ {
		h := cyclone.Cyclone{
			Num: i,
			Input: make(chan *erebos.Transport,
				conf.Cyclone.HandlerQueueLength),
			Shutdown: make(chan struct{}),
			Death:    handlerDeath,
			Config:   &conf,
			Metrics:  &pfxRegistry,
			Limit:    lim,
			AppLog:   logger,
		}
		h.SetLookup(lookup)
		cyclone.Handlers[i] = &h
		waitdelay.Use()
		go func() {
			defer waitdelay.Done()
			h.Start()
		}()
		logger.Infof("Launched Cyclone handler #%d", i)
	}

	// start kafka consumer
	waitdelay.Use()
	go func() {
		defer waitdelay.Done()
		erebos.Consumer(
			&conf,
			wrappedDispatch(&pfxRegistry, cyclone.Dispatch),
			consumerShutdown,
			consumerExit,
			handlerDeath,
		)
	}()

	heartbeat := time.Tick(10 * time.Second)

	// the main loop
	fault := false
runloop:
	for {
		select {
		case <-c:
			logger.Infoln(`Received shutdown signal`)
			break runloop
		case err := <-handlerDeath:
			logger.Errorf("Handler died: %s", err.Error())
			fault = true
			os.Exit(1) // die hard, somehow this stucks after errors..
			break runloop
		case <-heartbeat:
			for i := range cyclone.Handlers {
				// do not block on heartbeats
				waitdelay.Use()
				go func(i int) {
					cyclone.Handlers[i].InputChannel() <- erebos.NewHeartbeat()
					waitdelay.Done()
				}(i)
			}
		}
	}

	// close all handlers
	//	close(ms.Shutdown)
	go func() {
		time.Sleep(30 * time.Second)
		logger.Errorln("Could not shutdown cyclone correctly!")
		os.Exit(1)
	}()
	close(consumerShutdown)

	// not safe to close InputChannel before consumer is gone
	<-consumerExit
	for i := range cyclone.Handlers {
		close(cyclone.Handlers[i].ShutdownChannel())
		close(cyclone.Handlers[i].InputChannel())
	}

	// read all additional handler errors if required
drainloop:
	for {
		select {
		case err := <-handlerDeath:
			logger.Errorf("Handler died: %s", err.Error())
			fault = true
			os.Exit(1) // die hard, somehow this stucks after errors..
		case <-time.After(time.Millisecond * 10):
			break drainloop
		}
	}

	// give goroutines that were blocked on handlerDeath channel
	// a chance to exit
	waitdelay.Wait()
	logger.Infoln(`CYCLONE shutdown complete`)
	if fault {
		os.Exit(1)
	}
}

// redirectStderr to the file passed in
// this will allow us to log panics
func redirectStderr(f *os.File) {
	err := syscall.Dup2(int(f.Fd()), int(os.Stderr.Fd()))
	if err != nil {
		log.Fatalf("Failed to redirect stderr to file: %v", err)
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
