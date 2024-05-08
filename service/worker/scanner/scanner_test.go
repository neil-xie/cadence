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

package scanner

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/cadence/client"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/uber/cadence/common/resource"
	"go.uber.org/cadence/mocks"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"
)

type mockCadenceClient struct {
	client       *mocks.Client
	domainClient *mocks.DomainClient
}

func TestNewScanner(t *testing.T) {
	params := &BootstrapParams{}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockResource := resource.NewMockResource(ctrl)

	// Create the scanner using the mocked resource
	scanner := New(mockResource, params)

	// Assertions
	assert.NotNil(t, scanner, "Scanner should not be nil")
	assert.IsType(t, &Scanner{}, scanner, "Should return a *Scanner instance")
}

func TestStartScanner(t *testing.T) {
	params := &BootstrapParams{}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockResource := resource.NewMockResource(ctrl)

	// Create the scanner using the mocked resource
	scanner := New(mockResource, params)

	// Start the scanner
	err := scanner.Start()
	assert.NoError(t, err, "Start should not return an error")
}

func TestStartWorkflow(t *testing.T) {
	params := &BootstrapParams{}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockResource := resource.NewMockResource(ctrl)

	// Create the scanner using the mocked resource
	scanner := New(mockResource, params)

	clients := newMockCadenceClient()
	clients.client.On("StartWorkflow",
		mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Start the scanner
	err := scanner.startWorkflow(clients.client, client.StartWorkflowOptions{}, "test-workflow", nil)
	assert.NoError(t, err, "StartWorkflow should not return an error")
}

func TestNewScannerContext(t *testing.T) {
	context := NewScannerContext(context.Background(), "test-workflow", scannerContext{})
	assert.NotNil(t, context, "Scanner context should not be nil")
}

func TestGetScannerContext(t *testing.T) {
	ctx := NewScannerContext(context.Background(), "test-workflow", scannerContext{})
	s := &testsuite.WorkflowTestSuite{}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	env.RegisterActivity(getScannerContext)

	_, err := env.ExecuteActivity(getScannerContext)
	// this will fail since this workflow type is not valid
	assert.Error(t, err)
}

func newMockCadenceClient() mockCadenceClient {
	return mockCadenceClient{
		client:       new(mocks.Client),
		domainClient: new(mocks.DomainClient),
	}
}
