// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package benchmark runs replication benchmark integration tests.
package benchmark

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestBenchmark(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Replication Benchmark Suite", Label("integration", "slow", "benchmark"))
}

var repoRoot string

var _ = SynchronizedBeforeSuite(func() []byte {
	wd, err := os.Getwd()
	Expect(err).NotTo(HaveOccurred())
	repoRoot = filepath.Clean(filepath.Join(wd, "..", "..", "..", ".."))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	Expect(createKindCluster(ctx, repoRoot)).To(Succeed())
	Expect(buildLocalImage(ctx, repoRoot)).To(Succeed())
	Expect(loadImageToKind(ctx, localImage)).To(Succeed())
	Expect(loadImageToKind(ctx, localSlimImage)).To(Succeed())
	return []byte(repoRoot)
}, func(data []byte) {
	repoRoot = string(data)
})

var _ = SynchronizedAfterSuite(func() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	_ = deleteKindCluster(ctx)
}, func() {})
