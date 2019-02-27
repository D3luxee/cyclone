/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package cyclone // import "github.com/solnx/cyclone/internal/cyclone"

import (
	"fmt"
	"time"

	"github.com/go-resty/resty"
	"github.com/mjolnir42/delay"
	"github.com/mjolnir42/erebos"
	wall "github.com/solnx/eye/lib/eye.wall"
)

// Implementation of the erebos.Handler interface

//Start sets up the Cyclone application
func (c *Cyclone) Start() {
	if len(Handlers) == 0 {
		c.Death <- fmt.Errorf(`Incorrectly set handlers`)
		<-c.Shutdown
		return
	}

	c.client = resty.New()
	c.client = c.client.SetRedirectPolicy(
		resty.FlexibleRedirectPolicy(15)).
		SetDisableWarn(true).
		SetRetryCount(c.Config.Cyclone.RetryCount).
		SetRetryWaitTime(
			time.Duration(c.Config.Cyclone.RetryMinWaitTime)*
				time.Millisecond).
		SetRetryMaxWaitTime(
			time.Duration(c.Config.Cyclone.RetryMaxWaitTime)*
				time.Millisecond).
		SetHeader(`Content-Type`, `application/json`).
		SetContentLength(true)

	c.trackID = make(map[string]int)
	c.trackACK = make(map[string]*erebos.Transport)

	c.delay = delay.New()
	c.discard = make(map[string]bool)
	for _, path := range c.Config.Cyclone.DiscardMetrics {
		c.discard[path] = true
	}
	c.lookup = wall.NewLookup(c.Config, `cyclone`)
	if err := c.lookup.Start(); err != nil {
		c.Death <- err
		<-c.Shutdown
		return
	}
	defer c.lookup.Close()
	c.result = make(chan *alarmResult,
		c.Config.Cyclone.HandlerQueueLength,
	)
	c.run()
}

// InputChannel returns the data input channel
func (c *Cyclone) InputChannel() chan *erebos.Transport {
	return c.Input
}

// ShutdownChannel returns the shutdown signal channel
func (c *Cyclone) ShutdownChannel() chan struct{} {
	return c.Shutdown
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
