/*-
 * Copyright © 2016-2017, Jörg Pernfuß <code.jpe@gmail.com>
 * Copyright © 2016, 1&1 Internet SE
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package cyclone // import "github.com/solnx/cyclone/internal/cyclone"

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/patrickmn/go-cache"
	metrics "github.com/rcrowley/go-metrics"
)

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

// alarmResult encapsulates the result from a HTTP POST request
// to the AlarmAPI
type alarmResult struct {
	trackingID string
	err        error
	internal   bool
	alarm      *AlarmEvent
}

// process evaluates a metric and raises alarms as required. Must
// only be called as goroutine after c.delay.Use()
func (c *Cyclone) sendAlarm(a AlarmEvent, trackingID string) {
	//rate limit the amount of events sent
	// ok events every 3h and !=ok every 3min
	switch a.Level {
	case 0:
		if _, found := c.okCache.Get(a.EventID); found {
			c.result <- &alarmResult{
				trackingID: trackingID,
				err:        nil,
			}
			return
		}
		c.okCache.Set(a.EventID, true, cache.DefaultExpiration)
		c.errCache.Delete(a.EventID)
	default:
		if _, found := c.errCache.Get(a.EventID); found {
			c.result <- &alarmResult{
				trackingID: trackingID,
				err:        nil,
			}
			return
		}
		c.okCache.Delete(a.EventID)
		c.errCache.Set(a.EventID, true, cache.DefaultExpiration)
	}
	defer c.delay.Done()
	b := new(bytes.Buffer)
	aSlice := []AlarmEvent{a}
	if err := json.NewEncoder(b).Encode(aSlice); err != nil {
		c.AppLog.Errorf(
			"Cyclone[%d], ERROR json encoding alarm for %s: %s",
			c.Num, a.EventID, err,
		)
		c.result <- &alarmResult{
			trackingID: trackingID,
			err:        err,
			internal:   true,
			alarm:      &a,
		}
		return
	}
	r := c.client.SetTimeout(
		time.Duration(c.Config.Cyclone.RequestTimeout) *
			time.Millisecond).
		R()

	// acquire resource limit before issuing the POST request
	c.Limit.Start()
	resp, err := r.SetBody(b).
		Post(c.Config.Cyclone.DestinationURI)

	// release resource limit
	c.Limit.Done()
	if err != nil {
		c.AppLog.Errorf(
			"Cyclone[%d], ERROR sending alarm for %s: %s",
			c.Num, a.EventID, err,
		)
		c.result <- &alarmResult{
			trackingID: trackingID,
			err:        err,
			alarm:      &a,
		}
		return
	}
	c.AppLog.Infof(
		"Cyclone[%d], Dispatched alarm for %s at level %d, returncode was %d",
		c.Num, a.EventID, a.Level, resp.StatusCode,
	)
	if resp.StatusCode() >= 209 {
		// read response body
		bt := resp.Body()
		err = fmt.Errorf(
			"Cyclone[%d], ResponseMsg(%d): %s",
			c.Num, resp.StatusCode(), string(bt),
		)
		c.AppLog.Errorln(err.Error())

		// reset buffer and encode JSON again so it can be
		// logged
		b.Reset()
		json.NewEncoder(b).Encode(aSlice)
		// 4xx errors are caused on this side, abort
		if resp.StatusCode() < 500 {
			c.result <- &alarmResult{
				trackingID: trackingID,
				err:        err,
				internal:   true,
				alarm:      &a,
			}

			return
		}
		c.AppLog.Errorf(
			"Cyclone[%d], RequestJSON: %s",
			c.Num, b.String(),
		)
		c.result <- &alarmResult{
			trackingID: trackingID,
			err:        err,
			alarm:      &a,
		}
		return
	}
	// ensure http.Response.Body is consumed and closed
	_ = resp.Body()
	c.result <- &alarmResult{
		trackingID: trackingID,
		err:        nil,
	}
}

// resendAlarm raises the alarmapi.error metric and attempts to
// resend a every 5 seconds
func (c *Cyclone) resendAlarm(a *AlarmEvent, trackingID string) {
	metrics.GetOrRegisterGauge(`.alarmapi.error`,
		*c.Metrics).Update(1)
	broken := true
	retryCount := 0
	b := new(bytes.Buffer)
	aSlice := []AlarmEvent{*a}
	// encoding a previously did not cause an internal error
	if err := json.NewEncoder(b).Encode(aSlice); err != nil {
		c.AppLog.Errorf(
			"Cyclone[%d], ERROR json encoding alarm for %s: %s",
			c.Num, a.EventID, err,
		)
		c.result <- &alarmResult{
			trackingID: trackingID,
			err:        err,
			internal:   true,
			alarm:      a,
		}
		return
	}

	// fast first request attempt
	resendDelay := time.Millisecond * 50

	for broken {
		if retryCount > 5 {
			break
		}
		retryCount++
		select {
		// always listen for shutdown requests
		case <-c.Shutdown:
			return
		// attempt resend every 5 seconds
		case <-time.After(resendDelay):
		}
		// increase resendDelay for after first attempt
		resendDelay = 5 * time.Second

		r := c.client.SetTimeout(
			time.Duration(c.Config.Cyclone.RequestTimeout) *
				time.Millisecond).
			R()

		resp, err := r.SetBody(b).
			Post(c.Config.Cyclone.DestinationURI)

		if err != nil {
			c.AppLog.Errorf(
				"Cyclone[%d], ERROR sending alarm for %s: %s",
				c.Num, a.EventID, err,
			)
			continue
		}
		if resp.StatusCode() >= 209 {
			bt := resp.Body()
			c.AppLog.Errorf(
				"Cyclone[%d], ResponseMsg(%d): %s",
				c.Num, resp.StatusCode(), string(bt),
			)
			continue
		}
		broken = false
	}

	// switch error metric off
	metrics.GetOrRegisterGauge(`.alarmapi.error`,
		*c.Metrics).Update(0)

	// update offset directly since the result channel at this
	// point likely blocks
	//c.updateOffset(trackingID)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
