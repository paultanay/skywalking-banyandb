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
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type kubeList[T any] struct {
	Items []T `json:"items"`
}

type kubeOwnerRef struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type kubeMetadata struct {
	Name            string            `json:"name"`
	Namespace       string            `json:"namespace"`
	Labels          map[string]string `json:"labels"`
	OwnerReferences []kubeOwnerRef    `json:"ownerReferences"`
}

type kubeContainerPort struct {
	ContainerPort int `json:"containerPort"`
}

type kubeContainer struct {
	Ports []kubeContainerPort `json:"ports"`
}

type kubePodSpec struct {
	Containers []kubeContainer `json:"containers"`
}

type kubePod struct {
	Metadata kubeMetadata `json:"metadata"`
	Spec     kubePodSpec  `json:"spec"`
}

type kubeServicePort struct {
	Port int    `json:"port"`
	Name string `json:"name"`
}

type kubeServiceSpec struct {
	ClusterIP string            `json:"clusterIP"`
	Ports     []kubeServicePort `json:"ports"`
}

type kubeService struct {
	Metadata kubeMetadata    `json:"metadata"`
	Spec     kubeServiceSpec `json:"spec"`
}

func fetchPods(ctx context.Context, namespace string) ([]kubePod, error) {
	out, err := runCommand(ctx, "kubectl", "-n", namespace, "get", "pods", "-o", "json")
	if err != nil {
		return nil, err
	}
	return parsePodsJSON([]byte(out))
}

func fetchServices(ctx context.Context, namespace string) ([]kubeService, error) {
	out, err := runCommand(ctx, "kubectl", "-n", namespace, "get", "svc", "-o", "json")
	if err != nil {
		return nil, err
	}
	return parseServicesJSON([]byte(out))
}

func parsePodsJSON(data []byte) ([]kubePod, error) {
	var list kubeList[kubePod]
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func parseServicesJSON(data []byte) ([]kubeService, error) {
	var list kubeList[kubeService]
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func discoverDataPods(pods []kubePod) []kubePod {
	var dataPods []kubePod
	for _, pod := range pods {
		for _, owner := range pod.Metadata.OwnerReferences {
			if owner.Kind == "StatefulSet" && !strings.Contains(owner.Name, "etcd") {
				dataPods = append(dataPods, pod)
				break
			}
		}
	}
	sort.Slice(dataPods, func(i, j int) bool {
		return dataPods[i].Metadata.Name < dataPods[j].Metadata.Name
	})
	return dataPods
}

func discoverLiaisonPods(pods []kubePod) []kubePod {
	var liaisonPods []kubePod
	for _, pod := range pods {
		if pod.Metadata.Labels["app.kubernetes.io/component"] == "liaison" {
			liaisonPods = append(liaisonPods, pod)
			continue
		}
		for _, owner := range pod.Metadata.OwnerReferences {
			if owner.Kind == "ReplicaSet" || owner.Kind == "Deployment" || owner.Kind == "StatefulSet" {
				if strings.Contains(owner.Name, "etcd") || strings.Contains(owner.Name, "data") {
					continue
				}
				liaisonPods = append(liaisonPods, pod)
				break
			}
		}
	}
	sort.Slice(liaisonPods, func(i, j int) bool {
		return liaisonPods[i].Metadata.Name < liaisonPods[j].Metadata.Name
	})
	return liaisonPods
}

func discoverGRPCService(services []kubeService) (kubeService, error) {
	for _, svc := range services {
		if svc.Metadata.Labels["app.kubernetes.io/component"] != "liaison" {
			continue
		}
		if !serviceExposesPort(svc, 17912) {
			continue
		}
		if isHeadlessService(svc) {
			continue
		}
		return svc, nil
	}
	for _, svc := range services {
		if svc.Metadata.Labels["app.kubernetes.io/component"] != "liaison" {
			continue
		}
		if serviceExposesPort(svc, 17912) {
			return svc, nil
		}
	}
	for _, svc := range services {
		if strings.Contains(svc.Metadata.Name, "etcd") {
			continue
		}
		if serviceExposesPort(svc, 17912) && !isHeadlessService(svc) {
			return svc, nil
		}
	}
	for _, svc := range services {
		if strings.Contains(svc.Metadata.Name, "etcd") {
			continue
		}
		if serviceExposesPort(svc, 17912) {
			return svc, nil
		}
	}
	return kubeService{}, fmt.Errorf("no gRPC service exposing port 17912 found")
}

func serviceExposesPort(svc kubeService, port int) bool {
	for _, p := range svc.Spec.Ports {
		if p.Port == port {
			return true
		}
	}
	return false
}

func isHeadlessService(svc kubeService) bool {
	return strings.EqualFold(svc.Spec.ClusterIP, "none")
}
