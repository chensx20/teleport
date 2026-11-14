/*
Copyright 2020 Gravitational, Inc.

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
	"time"

	"github.com/gravitational/trace"
	"gopkg.in/check.v1"
)

type BenchmarkGeneratorSuite struct{}

var _ = check.Suite(&BenchmarkGeneratorSuite{})

func (s *BenchmarkGeneratorSuite) TestLinearRates(c *check.C) {
	tests := []struct {
		lower, upper, step int
		rates              []int
	}{
		{1, 5, 2, []int{1, 3, 5}},
		{1, 6, 2, []int{1, 3, 5}},
		{4, 4, 1, []int{4}},
		{2, 4, 5, []int{2}},
		{999999999, 1000000000, 1, []int{999999999, 1000000000}},
		{1, 1000000000, int(^uint(0) >> 1), []int{1}},
	}
	for _, test := range tests {
		generator, err := NewLinearBenchmarkGenerator(Benchmark{}, test.lower, test.upper, test.step)
		c.Assert(err, check.IsNil)
		for _, rate := range test.rates {
			benchmark, ok := generator.Next()
			c.Assert(ok, check.Equals, true)
			c.Assert(benchmark.Rate, check.Equals, rate)
		}
		for i := 0; i < 2; i++ {
			benchmark, ok := generator.Next()
			c.Assert(ok, check.Equals, false)
			c.Assert(benchmark, check.DeepEquals, Benchmark{})
		}
	}
}

func (s *BenchmarkGeneratorSuite) TestLinearValidation(c *check.C) {
	tests := []struct{ lower, upper, step int }{
		{0, 1, 1},
		{-1, 1, 1},
		{2, 1, 1},
		{1, 2, 0},
		{1, 2, -1},
		{1, 1000000001, 1},
	}
	for _, test := range tests {
		generator, err := NewLinearBenchmarkGenerator(Benchmark{}, test.lower, test.upper, test.step)
		c.Assert(generator, check.IsNil)
		c.Assert(trace.IsBadParameter(err), check.Equals, true)
	}
}

func (s *BenchmarkGeneratorSuite) TestLinearTemplateOwnership(c *check.C) {
	template := Benchmark{Threads: 3, Rate: 99, Duration: time.Minute, Command: []string{"echo", "hello"}, Interactive: true}
	generator, err := NewLinearBenchmarkGenerator(template, 1, 2, 1)
	c.Assert(err, check.IsNil)
	template.Command[0] = "changed"
	first, ok := generator.Next()
	c.Assert(ok, check.Equals, true)
	c.Assert(first, check.DeepEquals, Benchmark{Threads: 3, Rate: 1, Duration: time.Minute, Command: []string{"echo", "hello"}, Interactive: true})
	first.Command[1] = "changed"
	second, ok := generator.Next()
	c.Assert(ok, check.Equals, true)
	c.Assert(second, check.DeepEquals, Benchmark{Threads: 3, Rate: 2, Duration: time.Minute, Command: []string{"echo", "hello"}, Interactive: true})
	second.Command[0] = "changed again"
	c.Assert(first.Command, check.DeepEquals, []string{"echo", "changed"})

	for _, command := range [][]string{nil, {}} {
		generator, err := NewLinearBenchmarkGenerator(Benchmark{Command: command}, 1, 1, 1)
		c.Assert(err, check.IsNil)
		benchmark, ok := generator.Next()
		c.Assert(ok, check.Equals, true)
		c.Assert(benchmark.Command, check.DeepEquals, command)
	}
}
