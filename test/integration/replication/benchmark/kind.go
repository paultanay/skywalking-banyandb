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
	"context"
	"path/filepath"
)

const (
	kindClusterName = "banyandb-bench"
)

func createKindCluster(ctx context.Context, repoRoot string) error {
	configPath := filepath.Join(repoRoot, "test", "fodc", "kind.yaml")
	_, err := runCommand(ctx, "kind", "create", "cluster", "--name", kindClusterName, "--config", configPath)
	return err
}

func deleteKindCluster(ctx context.Context) error {
	_, err := runCommand(ctx, "kind", "delete", "cluster", "--name", kindClusterName)
	return err
}

func loadImageToKind(ctx context.Context, image string) error {
	_, err := runCommand(ctx, "kind", "load", "docker-image", image, "--name", kindClusterName)
	return err
}
