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

package integration_schema_test

// Deletion tests are automatically registered by importing the
// shared test cases package (casesschema) in schema_suite_test.go.
// The shared test cases are defined in test/cases/schema/deletion.go
// and cover measure, stream, and trace deletion verification
// in a distributed cluster environment (etcd + 2 data nodes + liaison node).
//
// See test/cases/schema/deletion.go for the 5-step verification process:
//   1. Write initial data to the target subject
//   2. Delete the target subject
//   3. Verify writes are rejected and subject is invisible
//   4. Write 20 batches to a different subject in the same group
//   5. Verify all written data can be retrieved successfully
