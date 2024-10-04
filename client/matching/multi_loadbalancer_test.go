// The MIT License (MIT)

// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package matching

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/uber/cadence/common/dynamicconfig"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/types"
)

func TestNewMultiLoadBalancer(t *testing.T) {
	ctrl := gomock.NewController(t)
	randomMock := NewMockLoadBalancer(ctrl)
	roundRobinMock := NewMockLoadBalancer(ctrl)
	domainIDToName := func(domainID string) (string, error) {
		return "testDomainName", nil
	}
	dc := dynamicconfig.NewCollection(dynamicconfig.NewNopClient(), testlogger.New(t))
	lb := NewMultiLoadBalancer(randomMock, roundRobinMock, domainIDToName, dc)
	assert.NotNil(t, lb)
	multiLB, ok := lb.(*multiLoadBalancer)
	assert.NotNil(t, multiLB)
	assert.True(t, ok)
	assert.NotNil(t, multiLB.random)
	assert.NotNil(t, multiLB.roundRobin)
	assert.NotNil(t, multiLB.domainIDToName)
	assert.NotNil(t, multiLB.loadbalancerStrategy)
}

func TestMultiLoadBalancer_PickWritePartition(t *testing.T) {

	// Mock the domainIDToName function
	domainIDToName := func(domainID string) (string, error) {
		if domainID == "valid-domain" {
			return "valid-domain-name", nil
		}
		return "", errors.New("domain not found")
	}

	// Test cases
	tests := []struct {
		name                 string
		domainID             string
		taskList             types.TaskList
		taskListType         int
		forwardedFrom        string
		loadbalancerStrategy string
		expectedPartition    string
	}{
		{
			name:                 "random partition when domainIDToName fails",
			domainID:             "invalid-domain",
			taskList:             types.TaskList{Name: "test-tasklist"},
			taskListType:         1,
			forwardedFrom:        "",
			loadbalancerStrategy: "random",
			expectedPartition:    "random-partition",
		},
		{
			name:                 "round-robin partition enabled",
			domainID:             "valid-domain",
			taskList:             types.TaskList{Name: "test-tasklist"},
			taskListType:         1,
			forwardedFrom:        "",
			loadbalancerStrategy: "round-robin",
			expectedPartition:    "roundrobin-partition",
		},
		{
			name:                 "random partition when round-robin disabled",
			domainID:             "valid-domain",
			taskList:             types.TaskList{Name: "test-tasklist"},
			taskListType:         1,
			forwardedFrom:        "",
			loadbalancerStrategy: "invalid-enum",
			expectedPartition:    "random-partition",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock behavior for random and round robin load balancers
			ctrl := gomock.NewController(t)

			// Mock the LoadBalancer interface
			randomMock := NewMockLoadBalancer(ctrl)
			roundRobinMock := NewMockLoadBalancer(ctrl)
			randomMock.EXPECT().PickWritePartition(tt.domainID, tt.taskList, tt.taskListType, tt.forwardedFrom).Return("random-partition").AnyTimes()
			roundRobinMock.EXPECT().PickWritePartition(tt.domainID, tt.taskList, tt.taskListType, tt.forwardedFrom).Return("roundrobin-partition").AnyTimes()

			loadbalancerStrategyFn := func(domainName, taskListName string, taskListType int) string {
				return tt.loadbalancerStrategy
			}

			// Create multiLoadBalancer
			lb := &multiLoadBalancer{
				random:               randomMock,
				roundRobin:           roundRobinMock,
				domainIDToName:       domainIDToName,
				loadbalancerStrategy: loadbalancerStrategyFn,
			}

			// Call PickWritePartition and assert result
			partition := lb.PickWritePartition(tt.domainID, tt.taskList, tt.taskListType, tt.forwardedFrom)
			assert.Equal(t, tt.expectedPartition, partition)
		})
	}
}

func TestMultiLoadBalancer_PickReadPartition(t *testing.T) {

	// Mock the domainIDToName function
	domainIDToName := func(domainID string) (string, error) {
		if domainID == "valid-domain" {
			return "valid-domain-name", nil
		}
		return "", errors.New("domain not found")
	}

	// Test cases
	tests := []struct {
		name                 string
		domainID             string
		taskList             types.TaskList
		taskListType         int
		forwardedFrom        string
		loadbalancerStrategy string
		expectedPartition    string
	}{
		{
			name:                 "random partition when domainIDToName fails",
			domainID:             "invalid-domain",
			taskList:             types.TaskList{Name: "test-tasklist"},
			taskListType:         1,
			forwardedFrom:        "",
			loadbalancerStrategy: "random",
			expectedPartition:    "random-partition",
		},
		{
			name:                 "round-robin partition enabled",
			domainID:             "valid-domain",
			taskList:             types.TaskList{Name: "test-tasklist"},
			taskListType:         1,
			forwardedFrom:        "",
			loadbalancerStrategy: "round-robin",
			expectedPartition:    "roundrobin-partition",
		},
		{
			name:                 "random partition when round-robin disabled",
			domainID:             "valid-domain",
			taskList:             types.TaskList{Name: "test-tasklist"},
			taskListType:         1,
			forwardedFrom:        "",
			loadbalancerStrategy: "invalid-enum",
			expectedPartition:    "random-partition",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock behavior for random and round robin load balancers
			ctrl := gomock.NewController(t)

			// Mock the LoadBalancer interface
			randomMock := NewMockLoadBalancer(ctrl)
			roundRobinMock := NewMockLoadBalancer(ctrl)
			randomMock.EXPECT().PickReadPartition(tt.domainID, tt.taskList, tt.taskListType, tt.forwardedFrom).Return("random-partition").AnyTimes()
			roundRobinMock.EXPECT().PickReadPartition(tt.domainID, tt.taskList, tt.taskListType, tt.forwardedFrom).Return("roundrobin-partition").AnyTimes()

			// Mock dynamic config for loadbalancer strategy
			loadbalancerStrategyFn := func(domainName, taskListName string, taskListType int) string {
				return tt.loadbalancerStrategy
			}

			// Create multiLoadBalancer
			lb := &multiLoadBalancer{
				random:               randomMock,
				roundRobin:           roundRobinMock,
				domainIDToName:       domainIDToName,
				loadbalancerStrategy: loadbalancerStrategyFn,
			}

			// Call PickReadPartition and assert result
			partition := lb.PickReadPartition(tt.domainID, tt.taskList, tt.taskListType, tt.forwardedFrom)
			assert.Equal(t, tt.expectedPartition, partition)
		})
	}
}