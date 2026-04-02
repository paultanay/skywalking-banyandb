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

package benchmark

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Report struct {
	GeneratedAt time.Time  `json:"generated_at"`
	Config      Config     `json:"config"`
	Results     []RFResult `json:"results"`
}

type RFResult struct {
	ReplicationFactor int            `json:"replication_factor"`
	Write             WriteResult    `json:"write"`
	Read              ReadResult     `json:"read"`
	Resources         ResourceReport `json:"resources"`
}

type WriteResult struct {
	TotalPoints   int     `json:"total_points"`
	DurationSec   float64 `json:"duration_sec"`
	ThroughputPps float64 `json:"throughput_points_per_sec"`
}

type ReadResult struct {
	Samples  int     `json:"samples"`
	MinMs    float64 `json:"min_ms"`
	MedianMs float64 `json:"median_ms"`
	P95Ms    float64 `json:"p95_ms"`
	P99Ms    float64 `json:"p99_ms"`
	MaxMs    float64 `json:"max_ms"`
}

type ResourceReport struct {
	WritePhase ResourcePhase `json:"write_phase"`
	ReadPhase  ResourcePhase `json:"read_phase"`
}

type ResourcePhase struct {
	Liaison ResourceStats `json:"liaison"`
	Data    ResourceStats `json:"data"`
}

type ResourceStats struct {
	PeakRSSBytes   int64   `json:"peak_rss_bytes"`
	PeakRSSPercent float64 `json:"peak_rss_percent"`
	MeanCPUPercent float64 `json:"mean_cpu_percent"`
	PeakCPUPercent float64 `json:"peak_cpu_percent"`
}

func writeReport(report Report, dir string) (string, error) {
	if dir == "" {
		resolved, err := resolveDefaultReportDir()
		if err != nil {
			return "", err
		}
		dir = resolved
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	filename := fmt.Sprintf("banyandb-benchmark-%s.json", time.Now().Format("20060102-150405"))
	path := filepath.Join(dir, filename)
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", err
	}
	return path, nil
}

func resolveDefaultReportDir() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	root := findRepoRoot(wd)
	if root == "" {
		return os.TempDir(), nil
	}
	return filepath.Dir(root), nil
}

func findRepoRoot(start string) string {
	dir := start
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}
