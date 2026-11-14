/*
Copyright 2017 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/gravitational/trace"
)

// Benchmark specifies benchmark requests to run
type Benchmark struct {
	// Threads is amount of concurrent execution threads to run
	Threads int
	// Rate is requests per second origination rate
	Rate int
	// Duration is test duration
	Duration time.Duration
	// Command is a command to run
	Command []string
	// Interactive turns on interactive sessions
	Interactive bool
}

// LinearBenchmarkGenerator produces benchmark configurations at increasing rates.
type LinearBenchmarkGenerator struct {
	template  Benchmark
	current   int
	upper     int
	step      int
	exhausted bool
}

// NewLinearBenchmarkGenerator creates a generator for rates from lower to upper.
func NewLinearBenchmarkGenerator(template Benchmark, lower, upper, step int) (*LinearBenchmarkGenerator, error) {
	if lower <= 0 {
		return nil, trace.BadParameter("lower rate must be positive")
	}
	if upper < lower {
		return nil, trace.BadParameter("upper rate must be at least the lower rate")
	}
	if upper > int(time.Second) {
		return nil, trace.BadParameter("upper rate must not exceed %v", int(time.Second))
	}
	if step <= 0 {
		return nil, trace.BadParameter("rate step must be positive")
	}
	template.Command = copyBenchmarkCommand(template.Command)
	return &LinearBenchmarkGenerator{template: template, current: lower, upper: upper, step: step}, nil
}

// Next returns the next benchmark configuration, or false when exhausted.
func (g *LinearBenchmarkGenerator) Next() (Benchmark, bool) {
	if g.exhausted {
		return Benchmark{}, false
	}
	benchmark := g.template
	benchmark.Rate = g.current
	benchmark.Command = copyBenchmarkCommand(g.template.Command)
	if g.step > g.upper-g.current {
		g.exhausted = true
	} else {
		g.current += g.step
	}
	return benchmark, true
}

func copyBenchmarkCommand(command []string) []string {
	if command == nil {
		return nil
	}
	commandCopy := make([]string, len(command))
	copy(commandCopy, command)
	return commandCopy
}

// BenchmarkResult is a result of the benchmark
type BenchmarkResult struct {
	// RequestsOriginated is amount of reuqests originated
	RequestsOriginated int
	// RequestsFailed is amount of requests failed
	RequestsFailed int
	// Histogram is a duration histogram
	Histogram *hdrhistogram.Histogram
	// LastError contains last recorded error
	LastError error
}

// Benchmark connects to remote server and executes requests in parallel according
// to benchmark spec. It returns benchmark result when completed.
// This is a blocking function that can be cancelled via context argument.
func (tc *TeleportClient) Benchmark(ctx context.Context, bench Benchmark) (*BenchmarkResult, error) {
	tc.Stdout = ioutil.Discard
	tc.Stderr = ioutil.Discard
	tc.Stdin = &bytes.Buffer{}

	ctx, cancel := context.WithTimeout(ctx, bench.Duration)
	defer cancel()

	requestC := make(chan *benchMeasure)
	responseC := make(chan *benchMeasure, bench.Threads)

	// create goroutines for concurrency
	for i := 0; i < bench.Threads; i++ {
		thread := &benchmarkThread{
			id:          i,
			ctx:         ctx,
			client:      tc,
			command:     bench.Command,
			interactive: bench.Interactive,
			receiveC:    requestC,
			sendC:       responseC,
		}
		go thread.run()
	}

	// producer goroutine
	go func() {
		interval := time.Duration(float64(1) / float64(bench.Rate) * float64(time.Second))
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// notice how we start the timer regardless of whether any goroutine can process it
				// this is to account for coordinated omission,
				// http://psy-lob-saw.blogspot.com/2015/03/fixing-ycsb-coordinated-omission.html
				measure := &benchMeasure{
					Start: time.Now(),
				}
				select {
				case requestC <- measure:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	var result BenchmarkResult
	// from one millisecond to 60000 milliseconds (minute) with 3 digits precision
	result.Histogram = hdrhistogram.New(1, 60000, 3)

	var doneThreads int
	var timeoutC <-chan time.Time
	doneC := ctx.Done()
	for {
		select {
		case <-timeoutC:
			result.LastError = trace.BadParameter("several requests hang: timeout waiting for %v threads to finish", bench.Threads-doneThreads)
			return &result, nil
		case <-doneC:
			// give it a couple of seconds to wrap up the goroutines,
			// set up the timer that will fire up if the all goroutines were not finished
			doneC = nil
			waitTime := time.Duration(result.Histogram.Max()) * time.Millisecond
			// going to wait latency + buffer to give requests in flight to wrap up
			waitTime = time.Duration(1.2 * float64(waitTime))
			timeoutC = time.After(waitTime)
		case measure := <-responseC:
			if measure.ThreadCompleted {
				doneThreads++
				if doneThreads == bench.Threads {
					return &result, nil
				}
			} else {
				if measure.Error != nil {
					result.RequestsFailed++
					result.LastError = measure.Error
				}
				result.RequestsOriginated++
				result.Histogram.RecordValue(int64(measure.End.Sub(measure.Start) / time.Millisecond))
			}
		}
	}

}

type benchMeasure struct {
	Start           time.Time
	End             time.Time
	ThreadCompleted bool
	ThreadID        int
	Error           error
}

type benchmarkThread struct {
	id          int
	ctx         context.Context
	client      *TeleportClient
	command     []string
	interactive bool
	receiveC    chan *benchMeasure
	sendC       chan *benchMeasure
}

func (b *benchmarkThread) execute(measure *benchMeasure) {
	if !b.interactive {
		// do not use parent context that will cancel in flight requests
		// because we give test some time to gracefully wrap up
		// the in-flight connections to avoid extra errors
		measure.Error = b.client.SSH(context.TODO(), nil, false)
		measure.End = time.Now()
		b.sendMeasure(measure)
		return
	}
	config := b.client.Config
	client, err := NewClient(&config)
	reader, writer := io.Pipe()
	client.Stdin = reader
	out := &bytes.Buffer{}
	client.Stdout = out
	client.Stderr = out
	if err != nil {
		measure.Error = err
		measure.End = time.Now()
		b.sendMeasure(measure)
		return
	}
	done := make(chan bool)
	go func() {
		measure.Error = b.client.SSH(b.ctx, nil, false)
		measure.End = time.Now()
		b.sendMeasure(measure)
		close(done)
	}()
	writer.Write([]byte(strings.Join(b.command, " ") + "\r\nexit\r\n"))
	<-done
}

func (b *benchmarkThread) sendMeasure(measure *benchMeasure) {
	measure.ThreadID = b.id
	select {
	case b.sendC <- measure:
	default:
		log.Warningf("blocked on measure send\n")
	}
}

func (b *benchmarkThread) run() {
	defer func() {
		if r := recover(); r != nil {
			log.Warningf("recover from panic: %v", r)
			b.sendMeasure(&benchMeasure{ThreadCompleted: true})
		}
	}()

	for {
		select {
		case measure := <-b.receiveC:
			b.execute(measure)
		case <-b.ctx.Done():
			b.sendMeasure(&benchMeasure{
				ThreadCompleted: true,
			})
			return
		}
	}
}
