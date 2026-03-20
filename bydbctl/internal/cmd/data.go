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

package cmd

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	stpb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/version"
)

const (
	defaultDataBatchSize = 1000
	defaultDataGRPCAddr  = "localhost:17912"
	dataGRPCConnTimeout  = 30 * time.Second
	dataKindStream       = "stream"
	dataKindMeasure      = "measure"
)

var (
	dataOutput    string
	dataInput     string
	dataKind      string
	dataBatchSize int
	dataGRPCAddr  string
)

// dataExportHeader is the first line of every NDJSON export file and carries schema metadata needed for import.
type dataExportHeader struct {
	Kind          string          `json:"kind"`
	Group         string          `json:"group"`
	Name          string          `json:"name"`
	TagFamilySpec []dataTagFamily `json:"tagFamilySpec"`
	FieldNames    []string        `json:"fieldNames,omitempty"`
}

// dataTagFamily records the ordered tag names for a single tag family.
type dataTagFamily struct {
	Name     string   `json:"name"`
	TagNames []string `json:"tagNames"`
}

func newDataCmd() *cobra.Command {
	dataCmd := &cobra.Command{
		Use:     "data",
		Version: version.Build(),
		Short:   "Data export/import operations",
	}

	exportCmd := &cobra.Command{
		Use:     "export",
		Version: version.Build(),
		Short:   "Export stream or measure data to a file in NDJSON format",
		Long:    timeRangeUsage,
		RunE:    runDataExport,
	}
	exportCmd.Flags().StringVarP(&dataKind, "kind", "k", dataKindStream, "Data kind: stream or measure")
	exportCmd.Flags().StringVarP(&dataOutput, "output", "o", "", "Output file path (default: stdout)")
	exportCmd.Flags().IntVar(&dataBatchSize, "batch-size", defaultDataBatchSize, "Number of records per query batch")
	bindNameFlag(exportCmd)
	bindTimeRangeFlag(exportCmd)

	importCmd := &cobra.Command{
		Use:     "import",
		Version: version.Build(),
		Short:   "Import data from a NDJSON file into a stream or measure",
		RunE:    runDataImport,
	}
	importCmd.Flags().StringVarP(&dataInput, "input", "i", "", "Input file path (default: stdin)")
	importCmd.Flags().StringVar(&dataGRPCAddr, "grpc-addr", defaultDataGRPCAddr, "gRPC server address in host:port format")

	bindTLSRelatedFlag(exportCmd, importCmd)
	dataCmd.AddCommand(exportCmd, importCmd)
	return dataCmd
}

func runDataExport(cmd *cobra.Command, _ []string) error {
	exportGroup := viper.GetString("group")
	if exportGroup == "" {
		return errors.New("please specify a group with -g/--group")
	}
	exportName := name
	if exportName == "" {
		return errors.New("please specify a name with -n/--name")
	}

	startTS, endTS, parseErr := parseExportTimeRange()
	if parseErr != nil {
		return parseErr
	}

	var out io.Writer
	if dataOutput == "" {
		out = cmd.OutOrStdout()
	} else {
		f, openErr := os.Create(dataOutput)
		if openErr != nil {
			return fmt.Errorf("failed to create output file %s: %w", dataOutput, openErr)
		}
		defer func() { _ = f.Close() }()
		out = f
	}

	switch dataKind {
	case dataKindStream:
		return exportStreamData(out, exportGroup, exportName, startTS, endTS)
	case dataKindMeasure:
		return exportMeasureData(out, exportGroup, exportName, startTS, endTS)
	default:
		return fmt.Errorf("unsupported data kind %q: must be %q or %q", dataKind, dataKindStream, dataKindMeasure)
	}
}

func parseExportTimeRange() (time.Time, time.Time, error) {
	var startTS, endTS time.Time
	var parseErr error
	switch {
	case start == "" && end == "":
		startTS = time.Now().Add(-30 * time.Minute)
		endTS = time.Now()
	case start != "" && end != "":
		if startTS, parseErr = parseTime(start); parseErr != nil {
			return time.Time{}, time.Time{}, parseErr
		}
		if endTS, parseErr = parseTime(end); parseErr != nil {
			return time.Time{}, time.Time{}, parseErr
		}
	case start != "":
		if startTS, parseErr = parseTime(start); parseErr != nil {
			return time.Time{}, time.Time{}, parseErr
		}
		endTS = startTS.Add(timeRange)
	default:
		if endTS, parseErr = parseTime(end); parseErr != nil {
			return time.Time{}, time.Time{}, parseErr
		}
		startTS = endTS.Add(-timeRange)
	}
	return startTS, endTS, nil
}

func makeDataRESTClient() (*resty.Client, error) {
	client := resty.New()
	if !enableTLS {
		return client, nil
	}
	tlsCfg := &tls.Config{
		// #nosec G402
		InsecureSkipVerify: insecure,
	}
	if cert != "" {
		certData, readErr := os.ReadFile(cert)
		if readErr != nil {
			return nil, fmt.Errorf("failed to read cert file: %w", readErr)
		}
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(certData) {
			return nil, errors.New("failed to add server's certificate")
		}
		tlsCfg.RootCAs = certPool
	}
	client.SetTLSClientConfig(tlsCfg)
	return client, nil
}

func doRESTGet(path string) ([]byte, error) {
	client, clientErr := makeDataRESTClient()
	if clientErr != nil {
		return nil, clientErr
	}
	req := client.R()
	authHdr := getAuthHeader()
	if authHdr != "" {
		req.Header.Set("Authorization", authHdr)
	}
	resp, reqErr := req.Get(getPath(path))
	if reqErr != nil {
		return nil, reqErr
	}
	body := resp.Body()
	var st stpb.Status
	if jsonErr := json.Unmarshal(body, &st); jsonErr == nil && st.Code != int32(codes.OK) {
		s := gstatus.FromProto(&st)
		return nil, s.Err()
	}
	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP status %d from %s", resp.StatusCode(), path)
	}
	return body, nil
}

func doRESTPost(path string, bodyData []byte) ([]byte, error) {
	client, clientErr := makeDataRESTClient()
	if clientErr != nil {
		return nil, clientErr
	}
	req := client.R()
	authHdr := getAuthHeader()
	if authHdr != "" {
		req.Header.Set("Authorization", authHdr)
	}
	resp, reqErr := req.SetBody(bodyData).Post(getPath(path))
	if reqErr != nil {
		return nil, reqErr
	}
	body := resp.Body()
	var st stpb.Status
	if jsonErr := json.Unmarshal(body, &st); jsonErr == nil && st.Code != int32(codes.OK) {
		s := gstatus.FromProto(&st)
		return nil, s.Err()
	}
	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP status %d from %s", resp.StatusCode(), path)
	}
	return body, nil
}

func exportStreamData(out io.Writer, exportGroup, exportName string, startTS, endTS time.Time) error {
	schemaBody, schemaErr := doRESTGet(fmt.Sprintf("/api/v1/stream/schema/%s/%s", exportGroup, exportName))
	if schemaErr != nil {
		return fmt.Errorf("failed to get stream schema: %w", schemaErr)
	}
	schemaResp := new(databasev1.StreamRegistryServiceGetResponse)
	if unmarshalErr := protojson.Unmarshal(schemaBody, schemaResp); unmarshalErr != nil {
		return fmt.Errorf("failed to parse stream schema: %w", unmarshalErr)
	}
	stream := schemaResp.Stream
	if stream == nil {
		return errors.New("stream schema response is empty")
	}

	tagFamilySpecs := make([]dataTagFamily, 0, len(stream.TagFamilies))
	for _, tf := range stream.TagFamilies {
		tagNames := make([]string, len(tf.Tags))
		for i, tag := range tf.Tags {
			tagNames[i] = tag.Name
		}
		tagFamilySpecs = append(tagFamilySpecs, dataTagFamily{Name: tf.Name, TagNames: tagNames})
	}

	hdr := dataExportHeader{
		Kind:          dataKindStream,
		Group:         exportGroup,
		Name:          exportName,
		TagFamilySpec: tagFamilySpecs,
	}
	hdrBytes, hdrErr := json.Marshal(hdr)
	if hdrErr != nil {
		return fmt.Errorf("failed to marshal export header: %w", hdrErr)
	}
	w := bufio.NewWriter(out)
	if _, writeErr := w.Write(hdrBytes); writeErr != nil {
		return fmt.Errorf("failed to write export header: %w", writeErr)
	}
	if _, writeErr := w.WriteString("\n"); writeErr != nil {
		return fmt.Errorf("failed to write newline: %w", writeErr)
	}

	projection := buildStreamProjection(tagFamilySpecs)
	if paginateErr := paginateStreamQuery(w, exportGroup, exportName, startTS, endTS, projection); paginateErr != nil {
		return paginateErr
	}
	return w.Flush()
}

func buildStreamProjection(specs []dataTagFamily) map[string]interface{} {
	families := make([]map[string]interface{}, 0, len(specs))
	for _, spec := range specs {
		families = append(families, map[string]interface{}{
			"name": spec.Name,
			"tags": spec.TagNames,
		})
	}
	return map[string]interface{}{"tagFamilies": families}
}

func paginateStreamQuery(w *bufio.Writer, exportGroup, exportName string, startTS, endTS time.Time, projection map[string]interface{}) error {
	marshaler := protojson.MarshalOptions{EmitUnpopulated: false}
	offset := 0
	for {
		queryBody := map[string]interface{}{
			"groups":   []string{exportGroup},
			"name":     exportName,
			"timeRange": map[string]interface{}{
				"begin": startTS.Format(time.RFC3339),
				"end":   endTS.Format(time.RFC3339),
			},
			"projection": projection,
			"limit":      dataBatchSize,
			"offset":     offset,
		}
		bodyBytes, marshalErr := json.Marshal(queryBody)
		if marshalErr != nil {
			return fmt.Errorf("failed to marshal stream query: %w", marshalErr)
		}
		respBytes, postErr := doRESTPost("/api/v1/stream/data", bodyBytes)
		if postErr != nil {
			return fmt.Errorf("stream query failed: %w", postErr)
		}
		qResp := new(streamv1.QueryResponse)
		if unmarshalErr := protojson.Unmarshal(respBytes, qResp); unmarshalErr != nil {
			return fmt.Errorf("failed to parse stream query response: %w", unmarshalErr)
		}
		for _, element := range qResp.Elements {
			elementBytes, elemErr := marshaler.Marshal(element)
			if elemErr != nil {
				return fmt.Errorf("failed to marshal element: %w", elemErr)
			}
			if _, writeErr := w.Write(elementBytes); writeErr != nil {
				return fmt.Errorf("failed to write element: %w", writeErr)
			}
			if _, writeErr := w.WriteString("\n"); writeErr != nil {
				return fmt.Errorf("failed to write newline: %w", writeErr)
			}
		}
		if len(qResp.Elements) < dataBatchSize {
			break
		}
		offset += len(qResp.Elements)
	}
	return nil
}

func exportMeasureData(out io.Writer, exportGroup, exportName string, startTS, endTS time.Time) error {
	schemaBody, schemaErr := doRESTGet(fmt.Sprintf("/api/v1/measure/schema/%s/%s", exportGroup, exportName))
	if schemaErr != nil {
		return fmt.Errorf("failed to get measure schema: %w", schemaErr)
	}
	schemaResp := new(databasev1.MeasureRegistryServiceGetResponse)
	if unmarshalErr := protojson.Unmarshal(schemaBody, schemaResp); unmarshalErr != nil {
		return fmt.Errorf("failed to parse measure schema: %w", unmarshalErr)
	}
	measure := schemaResp.Measure
	if measure == nil {
		return errors.New("measure schema response is empty")
	}

	tagFamilySpecs := make([]dataTagFamily, 0, len(measure.TagFamilies))
	for _, tf := range measure.TagFamilies {
		tagNames := make([]string, len(tf.Tags))
		for i, tag := range tf.Tags {
			tagNames[i] = tag.Name
		}
		tagFamilySpecs = append(tagFamilySpecs, dataTagFamily{Name: tf.Name, TagNames: tagNames})
	}
	fieldNames := make([]string, len(measure.Fields))
	for i, fs := range measure.Fields {
		fieldNames[i] = fs.Name
	}

	hdr := dataExportHeader{
		Kind:          dataKindMeasure,
		Group:         exportGroup,
		Name:          exportName,
		TagFamilySpec: tagFamilySpecs,
		FieldNames:    fieldNames,
	}
	hdrBytes, hdrErr := json.Marshal(hdr)
	if hdrErr != nil {
		return fmt.Errorf("failed to marshal export header: %w", hdrErr)
	}
	w := bufio.NewWriter(out)
	if _, writeErr := w.Write(hdrBytes); writeErr != nil {
		return fmt.Errorf("failed to write export header: %w", writeErr)
	}
	if _, writeErr := w.WriteString("\n"); writeErr != nil {
		return fmt.Errorf("failed to write newline: %w", writeErr)
	}

	tagProjection := buildMeasureTagProjection(tagFamilySpecs)
	if paginateErr := paginateMeasureQuery(w, exportGroup, exportName, startTS, endTS, tagProjection, fieldNames); paginateErr != nil {
		return paginateErr
	}
	return w.Flush()
}

func buildMeasureTagProjection(specs []dataTagFamily) map[string]interface{} {
	families := make([]map[string]interface{}, 0, len(specs))
	for _, spec := range specs {
		families = append(families, map[string]interface{}{
			"name": spec.Name,
			"tags": spec.TagNames,
		})
	}
	return map[string]interface{}{"tagFamilies": families}
}

func paginateMeasureQuery(
	w *bufio.Writer,
	exportGroup, exportName string,
	startTS, endTS time.Time,
	tagProjection map[string]interface{},
	fieldNames []string,
) error {
	marshaler := protojson.MarshalOptions{EmitUnpopulated: false}
	offset := 0
	for {
		queryBody := map[string]interface{}{
			"groups":   []string{exportGroup},
			"name":     exportName,
			"timeRange": map[string]interface{}{
				"begin": startTS.Format(time.RFC3339),
				"end":   endTS.Format(time.RFC3339),
			},
			"tagProjection":   tagProjection,
			"fieldProjection": map[string]interface{}{"names": fieldNames},
			"limit":           dataBatchSize,
			"offset":          offset,
		}
		bodyBytes, marshalErr := json.Marshal(queryBody)
		if marshalErr != nil {
			return fmt.Errorf("failed to marshal measure query: %w", marshalErr)
		}
		respBytes, postErr := doRESTPost("/api/v1/measure/data", bodyBytes)
		if postErr != nil {
			return fmt.Errorf("measure query failed: %w", postErr)
		}
		qResp := new(measurev1.QueryResponse)
		if unmarshalErr := protojson.Unmarshal(respBytes, qResp); unmarshalErr != nil {
			return fmt.Errorf("failed to parse measure query response: %w", unmarshalErr)
		}
		for _, dp := range qResp.DataPoints {
			dpBytes, dpErr := marshaler.Marshal(dp)
			if dpErr != nil {
				return fmt.Errorf("failed to marshal data point: %w", dpErr)
			}
			if _, writeErr := w.Write(dpBytes); writeErr != nil {
				return fmt.Errorf("failed to write data point: %w", writeErr)
			}
			if _, writeErr := w.WriteString("\n"); writeErr != nil {
				return fmt.Errorf("failed to write newline: %w", writeErr)
			}
		}
		if len(qResp.DataPoints) < dataBatchSize {
			break
		}
		offset += len(qResp.DataPoints)
	}
	return nil
}

func runDataImport(_ *cobra.Command, _ []string) error {
	var in io.Reader
	if dataInput == "" {
		in = os.Stdin
	} else {
		f, openErr := os.Open(dataInput)
		if openErr != nil {
			return fmt.Errorf("failed to open input file %s: %w", dataInput, openErr)
		}
		defer func() { _ = f.Close() }()
		in = f
	}

	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	if !scanner.Scan() {
		if scanErr := scanner.Err(); scanErr != nil {
			return fmt.Errorf("failed to read import header: %w", scanErr)
		}
		return errors.New("import file is empty")
	}
	hdrLine := scanner.Bytes()
	var hdr dataExportHeader
	if jsonErr := json.Unmarshal(hdrLine, &hdr); jsonErr != nil {
		return fmt.Errorf("failed to parse import header: %w", jsonErr)
	}

	opts, tlsErr := grpchelper.SecureOptions(nil, enableTLS, insecure, cert)
	if tlsErr != nil {
		return fmt.Errorf("failed to configure gRPC TLS: %w", tlsErr)
	}
	conn, dialErr := grpchelper.Conn(dataGRPCAddr, dataGRPCConnTimeout, opts...)
	if dialErr != nil {
		return fmt.Errorf("failed to connect to gRPC server at %s: %w", dataGRPCAddr, dialErr)
	}
	defer func() { _ = conn.Close() }()

	switch hdr.Kind {
	case dataKindStream:
		return importStreamData(conn, scanner, hdr)
	case dataKindMeasure:
		return importMeasureData(conn, scanner, hdr)
	default:
		return fmt.Errorf("unsupported data kind %q in import file", hdr.Kind)
	}
}

func importStreamData(conn *grpc.ClientConn, scanner *bufio.Scanner, hdr dataExportHeader) error {
	ctx := context.Background()
	writeClient, clientErr := streamv1.NewStreamServiceClient(conn).Write(ctx)
	if clientErr != nil {
		return fmt.Errorf("failed to create stream write client: %w", clientErr)
	}

	metadata := &commonv1.Metadata{Group: hdr.Group, Name: hdr.Name}
	tagFamilySpecs := buildStreamTagFamilySpecs(hdr.TagFamilySpec)

	var msgID uint64
	isFirst := true
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		element := new(streamv1.Element)
		if unmarshalErr := protojson.Unmarshal(line, element); unmarshalErr != nil {
			return fmt.Errorf("failed to unmarshal element: %w", unmarshalErr)
		}
		msgID++
		req := &streamv1.WriteRequest{
			Element:   convertStreamElement(element, hdr.TagFamilySpec),
			MessageId: msgID,
		}
		if isFirst {
			req.Metadata = metadata
			req.TagFamilySpec = tagFamilySpecs
			isFirst = false
		}
		if sendErr := writeClient.Send(req); sendErr != nil {
			return fmt.Errorf("failed to send stream write request: %w", sendErr)
		}
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return fmt.Errorf("failed to read stream import data: %w", scanErr)
	}

	if closeErr := writeClient.CloseSend(); closeErr != nil {
		return fmt.Errorf("failed to close stream write client: %w", closeErr)
	}
	for {
		_, recvErr := writeClient.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			return fmt.Errorf("stream write error: %w", recvErr)
		}
	}
	return nil
}

func buildStreamTagFamilySpecs(specs []dataTagFamily) []*streamv1.TagFamilySpec {
	result := make([]*streamv1.TagFamilySpec, 0, len(specs))
	for _, s := range specs {
		result = append(result, &streamv1.TagFamilySpec{Name: s.Name, TagNames: s.TagNames})
	}
	return result
}

func convertStreamElement(element *streamv1.Element, specs []dataTagFamily) *streamv1.ElementValue {
	familyByName := make(map[string]*modelv1.TagFamily, len(element.TagFamilies))
	for _, tf := range element.TagFamilies {
		familyByName[tf.Name] = tf
	}
	tagFamilies := make([]*modelv1.TagFamilyForWrite, 0, len(specs))
	for _, spec := range specs {
		family, found := familyByName[spec.Name]
		if !found {
			tagFamilies = append(tagFamilies, buildNullTagFamily(len(spec.TagNames)))
			continue
		}
		tagFamilies = append(tagFamilies, buildTagFamilyForWrite(spec.TagNames, family))
	}
	return &streamv1.ElementValue{
		ElementId:   element.ElementId,
		Timestamp:   element.Timestamp,
		TagFamilies: tagFamilies,
	}
}

func buildTagFamilyForWrite(tagNames []string, family *modelv1.TagFamily) *modelv1.TagFamilyForWrite {
	tagIndex := make(map[string]*modelv1.TagValue, len(family.Tags))
	for _, tag := range family.Tags {
		tagIndex[tag.Key] = tag.Value
	}
	tags := make([]*modelv1.TagValue, 0, len(tagNames))
	for _, tagName := range tagNames {
		if tv, ok := tagIndex[tagName]; ok {
			tags = append(tags, tv)
		} else {
			tags = append(tags, &modelv1.TagValue{Value: &modelv1.TagValue_Null{}})
		}
	}
	return &modelv1.TagFamilyForWrite{Tags: tags}
}

func buildNullTagFamily(size int) *modelv1.TagFamilyForWrite {
	tags := make([]*modelv1.TagValue, size)
	for i := range tags {
		tags[i] = &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}
	}
	return &modelv1.TagFamilyForWrite{Tags: tags}
}

func importMeasureData(conn *grpc.ClientConn, scanner *bufio.Scanner, hdr dataExportHeader) error {
	ctx := context.Background()
	writeClient, clientErr := measurev1.NewMeasureServiceClient(conn).Write(ctx)
	if clientErr != nil {
		return fmt.Errorf("failed to create measure write client: %w", clientErr)
	}

	metadata := &commonv1.Metadata{Group: hdr.Group, Name: hdr.Name}
	tagFamilySpecs := buildMeasureTagFamilySpecs(hdr.TagFamilySpec)

	var msgID uint64
	isFirst := true
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		dp := new(measurev1.DataPoint)
		if unmarshalErr := protojson.Unmarshal(line, dp); unmarshalErr != nil {
			return fmt.Errorf("failed to unmarshal data point: %w", unmarshalErr)
		}
		msgID++
		req := &measurev1.WriteRequest{
			DataPoint: convertMeasureDataPoint(dp, hdr.TagFamilySpec, hdr.FieldNames),
			MessageId: msgID,
		}
		if isFirst {
			req.Metadata = metadata
			req.DataPointSpec = &measurev1.DataPointSpec{
				TagFamilySpec: tagFamilySpecs,
				FieldNames:    hdr.FieldNames,
			}
			isFirst = false
		}
		if sendErr := writeClient.Send(req); sendErr != nil {
			return fmt.Errorf("failed to send measure write request: %w", sendErr)
		}
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return fmt.Errorf("failed to read measure import data: %w", scanErr)
	}

	if closeErr := writeClient.CloseSend(); closeErr != nil {
		return fmt.Errorf("failed to close measure write client: %w", closeErr)
	}
	for {
		_, recvErr := writeClient.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			return fmt.Errorf("measure write error: %w", recvErr)
		}
	}
	return nil
}

func buildMeasureTagFamilySpecs(specs []dataTagFamily) []*measurev1.TagFamilySpec {
	result := make([]*measurev1.TagFamilySpec, 0, len(specs))
	for _, s := range specs {
		result = append(result, &measurev1.TagFamilySpec{Name: s.Name, TagNames: s.TagNames})
	}
	return result
}

func convertMeasureDataPoint(dp *measurev1.DataPoint, specs []dataTagFamily, fieldNames []string) *measurev1.DataPointValue {
	familyByName := make(map[string]*modelv1.TagFamily, len(dp.TagFamilies))
	for _, tf := range dp.TagFamilies {
		familyByName[tf.Name] = tf
	}
	tagFamilies := make([]*modelv1.TagFamilyForWrite, 0, len(specs))
	for _, spec := range specs {
		family, found := familyByName[spec.Name]
		if !found {
			tagFamilies = append(tagFamilies, buildNullTagFamily(len(spec.TagNames)))
			continue
		}
		tagFamilies = append(tagFamilies, buildTagFamilyForWrite(spec.TagNames, family))
	}

	fieldByName := make(map[string]*modelv1.FieldValue, len(dp.Fields))
	for _, f := range dp.Fields {
		fieldByName[f.Name] = f.Value
	}
	fields := make([]*modelv1.FieldValue, 0, len(fieldNames))
	for _, fieldName := range fieldNames {
		if fv, ok := fieldByName[fieldName]; ok {
			fields = append(fields, fv)
		} else {
			fields = append(fields, &modelv1.FieldValue{Value: &modelv1.FieldValue_Null{}})
		}
	}

	return &measurev1.DataPointValue{
		Timestamp:   dp.Timestamp,
		TagFamilies: tagFamilies,
		Fields:      fields,
		Version:     dp.Version,
	}
}
