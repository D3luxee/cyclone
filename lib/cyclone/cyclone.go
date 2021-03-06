/*-
 * Copyright © 2016-2017, Jörg Pernfuß <code.jpe@gmail.com>
 * Copyright © 2016, 1&1 Internet SE
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package cyclone // import "github.com/mjolnir42/cyclone/lib/cyclone"

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/go-redis/redis"
	"github.com/mjolnir42/cyclone/lib/cyclone/cpu"
	"github.com/mjolnir42/cyclone/lib/cyclone/disk"
	"github.com/mjolnir42/cyclone/lib/cyclone/mem"
	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/legacy"
	metrics "github.com/rcrowley/go-metrics"
)

// Handlers is the registry of running application handlers
var Handlers map[int]erebos.Handler

// AgeCutOff is the duration after which back-processed alarms are
// ignored and not alerted
var AgeCutOff time.Duration

func init() {
	Handlers = make(map[int]erebos.Handler)
}

// Cyclone performs threshold evaluation alarming on metrics
type Cyclone struct {
	Num           int
	Input         chan *erebos.Transport
	Shutdown      chan struct{}
	Death         chan error
	Config        *erebos.Config
	Metrics       *metrics.Registry
	CPUData       map[int64]cpu.CPU
	MemData       map[int64]mem.Mem
	CTXData       map[int64]cpu.CTX
	DskData       map[int64]map[string]disk.Disk
	redis         *redis.Client
	internalInput chan *legacy.MetricSplit
}

// AlarmEvent is the datatype for sending out alarm notifications
type AlarmEvent struct {
	Source     string `json:"source"`
	EventID    string `json:"event_id"`
	Version    string `json:"version"`
	Sourcehost string `json:"sourcehost"`
	Oncall     string `json:"on_call"`
	Targethost string `json:"targethost"`
	Message    string `json:"message"`
	Level      int64  `json:"level"`
	Timestamp  string `json:"timestamp"`
	Check      string `json:"check"`
	Monitoring string `json:"monitoring"`
	Team       string `json:"team"`
}

// run is the event loop for Cyclone
func (c *Cyclone) run() {

runloop:
	for {
		select {
		case <-c.Shutdown:
			// received shutdown, drain input channel which will be
			// closed by main
			goto drainloop
		case msg := <-c.Input:
			if msg == nil {
				// this can happen if we read the closed Input channel
				// before the closed Shutdown channel
				continue runloop
			}
			if err := c.process(msg); err != nil {
				c.Death <- err
				<-c.Shutdown
				break runloop
			}
		}
	}

drainloop:
	for {
		select {
		case msg := <-c.Input:
			if msg == nil {
				// channel is closed
				break drainloop
			}
			c.process(msg)
		}
	}
}

// process evaluates a metric and raises alarms as required
func (c *Cyclone) process(msg *erebos.Transport) error {
	if msg == nil || msg.Value == nil {
		logrus.Warnf("Ignoring empty message from: %d", msg.HostID)
		if msg != nil {
			go c.commit(msg)
		}
		return nil
	}

	m := &legacy.MetricSplit{}
	if err := json.Unmarshal(msg.Value, m); err != nil {
		return err
	}

	switch m.Path {
	case `_internal.cyclone.heartbeat`:
		c.heartbeat()
		return nil
	}

	// non-heartbeat metrics count towards processed metrics
	metrics.GetOrRegisterMeter(`/metrics/processed.per.second`,
		*c.Metrics).Mark(1)

	switch m.Path {
	case `/sys/cpu/ctx`:
		ctx := cpu.CTX{}
		id := m.AssetID
		if _, ok := c.CTXData[id]; ok {
			ctx = c.CTXData[id]
		}
		m = ctx.Update(m)
		c.CTXData[id] = ctx

	case `/sys/cpu/count/idle`:
		fallthrough
	case `/sys/cpu/count/iowait`:
		fallthrough
	case `/sys/cpu/count/irq`:
		fallthrough
	case `/sys/cpu/count/nice`:
		fallthrough
	case `/sys/cpu/count/softirq`:
		fallthrough
	case `/sys/cpu/count/system`:
		fallthrough
	case `/sys/cpu/count/user`:
		cu := cpu.CPU{}
		id := m.AssetID
		if _, ok := c.CPUData[id]; ok {
			cu = c.CPUData[id]
		}
		cu.Update(m)
		m = cu.Calculate()
		c.CPUData[id] = cu

	case `/sys/memory/active`:
		fallthrough
	case `/sys/memory/buffers`:
		fallthrough
	case `/sys/memory/cached`:
		fallthrough
	case `/sys/memory/free`:
		fallthrough
	case `/sys/memory/inactive`:
		fallthrough
	case `/sys/memory/swapfree`:
		fallthrough
	case `/sys/memory/swaptotal`:
		fallthrough
	case `/sys/memory/total`:
		mm := mem.Mem{}
		id := m.AssetID
		if _, ok := c.MemData[id]; ok {
			mm = c.MemData[id]
		}
		mm.Update(m)
		m = mm.Calculate()
		c.MemData[id] = mm

	case `/sys/disk/blk_total`:
		fallthrough
	case `/sys/disk/blk_used`:
		fallthrough
	case `/sys/disk/blk_read`:
		fallthrough
	case `/sys/disk/blk_wrtn`:
		if len(m.Tags) == 0 {
			m = nil
			break
		}
		d := disk.Disk{}
		id := m.AssetID
		mpt := m.Tags[0]
		if c.DskData[id] == nil {
			c.DskData[id] = make(map[string]disk.Disk)
		}
		if _, ok := c.DskData[id][mpt]; !ok {
			c.DskData[id][mpt] = d
		}
		if _, ok := c.DskData[id][mpt]; ok {
			d = c.DskData[id][mpt]
		}
		d.Update(m)
		mArr := d.Calculate()
		if mArr != nil {
			for _, mPtr := range mArr {
				// no deadlock, channel is buffered
				c.internalInput <- mPtr
			}
		}
		c.DskData[id][mpt] = d
		m = nil
	}

	if m == nil {
		logrus.Debugf("Cyclone[%d], Metric has been consumed", c.Num)
		return nil
	}

	lid := m.LookupID()
	thr := c.Lookup(lid)
	if thr == nil {
		logrus.Errorf("Cyclone[%d], ERROR fetching threshold data. Lookup service available?", c.Num)
		return nil
	}
	if len(thr) == 0 {
		logrus.Debugf("Cyclone[%d], No thresholds configured for %s from %d", c.Num, m.Path, m.AssetID)
		return nil
	}
	logrus.Debugf("Cyclone[%d], Forwarding %s from %d for evaluation (%s)", c.Num, m.Path, m.AssetID, lid)
	evals := metrics.GetOrRegisterMeter(`/evaluations.per.second`,
		*c.Metrics)
	evals.Mark(1)

	internalMetric := false
	switch m.Path {
	case
		// internal metrics generated by cyclone
		`cpu.ctx.per.second`,
		`cpu.usage.percent`,
		`memory.usage.percent`:
		internalMetric = true
	case
		// internal metrics sent by main daemon
		`/sys/cpu/blocked`,
		`/sys/cpu/uptime`,
		`/sys/load/300s`,
		`/sys/load/60s`,
		`/sys/load/900s`,
		`/sys/load/running_proc`,
		`/sys/load/total_proc`:
		internalMetric = true
	default:
		switch {
		case
			strings.HasPrefix(m.Path, `disk.free:`),
			strings.HasPrefix(m.Path, `disk.read.per.second:`),
			strings.HasPrefix(m.Path, `disk.usage.percent:`),
			strings.HasPrefix(m.Path, `disk.write.per.second:`):
			internalMetric = true
		}
	}

	evaluations := 0

thrloop:
	for key := range thr {
		var alarmLevel = "0"
		var brokenThr int64
		dispatchAlarm := false
		broken := false
		fVal := ``
		if internalMetric {
			dispatchAlarm = true
		}
		if len(m.Tags) > 0 && m.Tags[0] == thr[key].ID {
			dispatchAlarm = true
		}
		if !dispatchAlarm {
			continue thrloop
		}
		logrus.Debugf("Cyclone[%d], Evaluating metric %s from %d against config %s",
			c.Num, m.Path, m.AssetID, thr[key].ID)
		evaluations++

	lvlloop:
		for _, lvl := range []string{`9`, `8`, `7`, `6`, `5`, `4`, `3`, `2`, `1`, `0`} {
			thrval, ok := thr[key].Thresholds[lvl]
			if !ok {
				continue
			}
			logrus.Debugf("Cyclone[%d], Checking %s alarmlevel %s", c.Num, thr[key].ID, lvl)
			switch m.Type {
			case `integer`:
				fallthrough
			case `long`:
				broken, fVal = c.cmpInt(thr[key].Predicate,
					m.Value().(int64),
					thrval)
			case `real`:
				broken, fVal = c.cmpFlp(thr[key].Predicate,
					m.Value().(float64),
					thrval)
			}
			if broken {
				alarmLevel = lvl
				brokenThr = thrval
				break lvlloop
			}
		}
		al := AlarmEvent{
			Source:     fmt.Sprintf("%s / %s", thr[key].MetaTargethost, thr[key].MetaSource),
			EventID:    thr[key].ID,
			Version:    c.Config.Cyclone.APIVersion,
			Sourcehost: thr[key].MetaTargethost,
			Oncall:     thr[key].Oncall,
			Targethost: thr[key].MetaTargethost,
			Timestamp:  time.Now().UTC().Format(time.RFC3339Nano),
			Check:      fmt.Sprintf("cyclone(%s)", m.Path),
			Monitoring: thr[key].MetaMonitoring,
			Team:       thr[key].MetaTeam,
		}
		al.Level, _ = strconv.ParseInt(alarmLevel, 10, 64)
		if alarmLevel == `0` {
			al.Message = `Ok.`
		} else {
			al.Message = fmt.Sprintf(
				"Metric %s has broken threshold. Value %s %s %d",
				m.Path,
				fVal,
				thr[key].Predicate,
				brokenThr,
			)
		}
		if al.Oncall == `` {
			al.Oncall = `No oncall information available`
		}
		c.updateEval(thr[key].ID)
		if c.Config.Cyclone.TestMode {
			// do not send out alarms in testmode
			continue thrloop
		}
		alrms := metrics.GetOrRegisterMeter(`/alarms.per.second`,
			*c.Metrics)
		alrms.Mark(1)
		go func(a AlarmEvent) {
			b := new(bytes.Buffer)
			aSlice := []AlarmEvent{a}
			if err := json.NewEncoder(b).Encode(aSlice); err != nil {
				logrus.Errorf("Cyclone[%d], ERROR json encoding alarm for %s: %s", c.Num, a.EventID, err)
				return
			}
			resp, err := http.Post(
				c.Config.Cyclone.DestinationURI,
				`application/json; charset=utf-8`,
				b,
			)

			if err != nil {
				logrus.Errorf("Cyclone[%d], ERROR sending alarm for %s: %s", c.Num, a.EventID, err)
				return
			}
			logrus.Infof("Cyclone[%d], Dispatched alarm for %s at level %d, returncode was %d",
				c.Num, a.EventID, a.Level, resp.StatusCode)
			if resp.StatusCode >= 209 {
				// read response body
				bt, _ := ioutil.ReadAll(resp.Body)
				logrus.Errorf("Cyclone[%d], ResponseMsg(%d): %s", c.Num, resp.StatusCode, string(bt))
				resp.Body.Close()

				// reset buffer and encode JSON again so it can be
				// logged
				b.Reset()
				json.NewEncoder(b).Encode(aSlice)
				logrus.Errorf("Cyclone[%d], RequestJSON: %s", c.Num, b.String())
				return
			}
			// ensure http.Response.Body is consumed and closed,
			// otherwise it leaks filehandles
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}(al)
	}
	if evaluations == 0 {
		logrus.Debugf("Cyclone[%d], metric %s(%d) matched no configurations", c.Num, m.Path, m.AssetID)
	}
	return nil
}

// commit marks a message as fully processed
func (c *Cyclone) commit(msg *erebos.Transport) {
	msg.Commit <- &erebos.Commit{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}
}

// cmpInt compares an integer value against a threshold
func (c *Cyclone) cmpInt(pred string, value, threshold int64) (bool, string) {
	fVal := fmt.Sprintf("%d", value)
	switch pred {
	case `<`:
		return value < threshold, fVal
	case `<=`:
		return value <= threshold, fVal
	case `==`:
		return value == threshold, fVal
	case `>=`:
		return value >= threshold, fVal
	case `>`:
		return value > threshold, fVal
	case `!=`:
		return value != threshold, fVal
	default:
		logrus.Errorf("Cyclone[%d], ERROR unknown predicate: %s", c.Num, pred)
		return false, ``
	}
}

// cmpFlp compares a floating point value against a threshold
func (c *Cyclone) cmpFlp(pred string, value float64, threshold int64) (bool, string) {
	fthreshold := float64(threshold)
	fVal := fmt.Sprintf("%.3f", value)
	switch pred {
	case `<`:
		return value < fthreshold, fVal
	case `<=`:
		return value <= fthreshold, fVal
	case `==`:
		return value == fthreshold, fVal
	case `>=`:
		return value >= fthreshold, fVal
	case `>`:
		return value > fthreshold, fVal
	case `!=`:
		return value != fthreshold, fVal
	default:
		logrus.Errorf("Cyclone[%d], ERROR unknown predicate: %s", c.Num, pred)
		return false, ``
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
