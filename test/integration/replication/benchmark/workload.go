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
	"fmt"
	"io"
	"math"
	"math/rand"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
)

const (
	benchGroupName   = "bench_measure_group"
	benchMeasure     = "bench_measure"
	benchTagFamily   = "default"
	benchTagName     = "entity"
	benchFieldName   = "value"
	queryLimit       = 100
	stabilizeTimeout = 3 * time.Minute
)

func createMeasureSchema(ctx context.Context, conn *grpc.ClientConn, rf int) error {
	groupClient := databasev1.NewGroupRegistryServiceClient(conn)
	_, err := groupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
		Group: &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: benchGroupName},
			Catalog:  commonv1.Catalog_CATALOG_MEASURE,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum:        uint32(3),
				Replicas:        uint32(rf),
				SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
				Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
			},
		},
	})
	if err != nil {
		return err
	}

	measureClient := databasev1.NewMeasureRegistryServiceClient(conn)
	_, err = measureClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
		Measure: &databasev1.Measure{
			Metadata: &commonv1.Metadata{Name: benchMeasure, Group: benchGroupName},
			Entity:   &databasev1.Entity{TagNames: []string{benchTagName}},
			TagFamilies: []*databasev1.TagFamilySpec{{
				Name: benchTagFamily,
				Tags: []*databasev1.TagSpec{{
					Name: benchTagName,
					Type: databasev1.TagType_TAG_TYPE_STRING,
				}},
			}},
			Fields: []*databasev1.FieldSpec{{
				Name:              benchFieldName,
				FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			}},
		},
	})
	return err
}

func writeMeasureData(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time) (WriteResult, error) {
	client := measurev1.NewMeasureServiceClient(conn)
	writers := cfg.Writers
	entities := cfg.Entities
	points := cfg.PointsPerEntity

	entityPerWriter := int(math.Ceil(float64(entities) / float64(writers)))
	group, gctx := errgroup.WithContext(ctx)
	start := time.Now()

	for w := 0; w < writers; w++ {
		writerIndex := w
		startEntity := writerIndex * entityPerWriter
		endEntity := (writerIndex + 1) * entityPerWriter
		if endEntity > entities {
			endEntity = entities
		}
		if startEntity >= endEntity {
			continue
		}
		group.Go(func() error {
			stream, err := client.Write(gctx)
			if err != nil {
				return err
			}
			spec := &measurev1.DataPointSpec{
				TagFamilySpec: []*measurev1.TagFamilySpec{{
					Name:     benchTagFamily,
					TagNames: []string{benchTagName},
				}},
				FieldNames: []string{benchFieldName},
			}
			metadata := &commonv1.Metadata{Name: benchMeasure, Group: benchGroupName}
			messageID := uint64(time.Now().UnixNano())
			for entityIdx := startEntity; entityIdx < endEntity; entityIdx++ {
				entityID := fmt.Sprintf("entity-%d", entityIdx)
				for pointIdx := 0; pointIdx < points; pointIdx++ {
					req := &measurev1.WriteRequest{
						Metadata:      metadata,
						DataPointSpec: spec,
						DataPoint: &measurev1.DataPointValue{
							Timestamp: timestamppb.New(base.Add(time.Duration(pointIdx) * time.Second)),
							TagFamilies: []*modelv1.TagFamilyForWrite{{
								Tags: []*modelv1.TagValue{{
									Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entityID}},
								}},
							}},
							Fields: []*modelv1.FieldValue{{
								Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(pointIdx)}},
							}},
						},
						MessageId: messageID,
					}
					if err := stream.Send(req); err != nil {
						return err
					}
					messageID++
					metadata = nil
					spec = nil
				}
			}
			if err := stream.CloseSend(); err != nil {
				return err
			}
			for {
				_, err := stream.Recv()
				if err != nil {
					if err == io.EOF {
						return nil
					}
					return err
				}
			}
		})
	}

	if err := group.Wait(); err != nil {
		return WriteResult{}, err
	}
	elapsed := time.Since(start)
	totalPoints := entities * points
	return WriteResult{
		TotalPoints:   totalPoints,
		DurationSec:   elapsed.Seconds(),
		ThroughputPps: float64(totalPoints) / elapsed.Seconds(),
	}, nil
}

func waitForVisibility(ctx context.Context, conn *grpc.ClientConn, base time.Time, expected int) error {
	client := measurev1.NewMeasureServiceClient(conn)
	deadline := time.Now().Add(stabilizeTimeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		req := buildQueryRequest(base, "entity-0")
		resp, err := client.Query(ctx, req)
		if err == nil && len(resp.DataPoints) >= expected {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timeout waiting for data visibility")
}

func runReadQueries(ctx context.Context, conn *grpc.ClientConn, cfg Config, base time.Time) ([]time.Duration, error) {
	client := measurev1.NewMeasureServiceClient(conn)
	nTargets := int(math.Min(20, float64(cfg.Entities)))
	entityTargets := make([]string, nTargets)
	for i := 0; i < nTargets; i++ {
		entityTargets[i] = fmt.Sprintf("entity-%d", i)
	}
	iterations := cfg.QueryIterations
	workers := cfg.QueryWorkers
	if workers <= 0 {
		workers = 1
	}
	jobs := make(chan int, iterations)
	for i := 0; i < iterations; i++ {
		jobs <- i
	}
	close(jobs)

	group, gctx := errgroup.WithContext(ctx)
	results := make(chan time.Duration, iterations)
	for w := 0; w < workers; w++ {
		seed := int64(42 + w)
		group.Go(func() error {
			rng := rand.New(rand.NewSource(seed))
			for range jobs {
				entity := entityTargets[rng.Intn(len(entityTargets))]
				req := buildQueryRequest(base, entity)
				start := time.Now()
				_, err := client.Query(gctx, req)
				if err != nil {
					return err
				}
				results <- time.Since(start)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	close(results)
	var durations []time.Duration
	for d := range results {
		durations = append(durations, d)
	}
	return durations, nil
}

func buildQueryRequest(base time.Time, entity string) *measurev1.QueryRequest {
	return &measurev1.QueryRequest{
		Groups: []string{benchGroupName},
		Name:   benchMeasure,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(base.Add(-time.Minute)),
			End:   timestamppb.New(base.Add(48 * time.Hour)),
		},
		Criteria: &modelv1.Criteria{
			Exp: &modelv1.Criteria_Condition{
				Condition: &modelv1.Condition{
					Name:  benchTagName,
					Op:    modelv1.Condition_BINARY_OP_EQ,
					Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entity}}},
				},
			},
		},
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{{
				Name: benchTagFamily,
				Tags: []string{benchTagName},
			}},
		},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{benchFieldName}},
		Limit:           queryLimit,
	}
}

func connectGRPC(addr string) (*grpc.ClientConn, error) {
	return grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
}
