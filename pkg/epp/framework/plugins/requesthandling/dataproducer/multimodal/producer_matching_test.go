/*
Copyright 2026 The llm-d Authors.

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

package multimodal

import (
	"context"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	fwkrequest "github.com/llm-d/llm-d-router/pkg/epp/framework/common/request"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrmm "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/multimodal"
)

func TestProducePreservesLookupMetricsAndMatches(t *testing.T) {
	podA := k8stypes.NamespacedName{Namespace: "default", Name: "pod-a"}
	podB := k8stypes.NamespacedName{Namespace: "default", Name: "pod-b"}
	podC := k8stypes.NamespacedName{Namespace: "default", Name: "pod-c"}
	items := []attrmm.MatchItem{
		{Hash: "image-a", Size: 80, Modality: string(reqcommon.ModalityImage)},
		{Hash: "video-a", Size: 20, Modality: string(reqcommon.ModalityVideo)},
		{Hash: "audio-a", Size: 5, Modality: string(reqcommon.ModalityAudio)},
	}
	tests := []struct {
		name       string
		candidates []k8stypes.NamespacedName
	}{
		{name: "all pods", candidates: []k8stypes.NamespacedName{podA, podB, podC}},
		{name: "subset of pods", candidates: []k8stypes.NamespacedName{podA}},
		{name: "no candidates"},
		{name: "uncached pod", candidates: []k8stypes.NamespacedName{podC}},
		{name: "repeated pod", candidates: []k8stypes.NamespacedName{podA, podA}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestProducer(t, nil, nil)
			p.putCacheEntry("image-a", podA, podB)
			p.putCacheEntry("video-a", podA)
			beforeCache := p.cacheSnapshot()
			request := &scheduling.InferenceRequest{
				RequestID: "matching-request",
				Body: &fwkrequest.InferenceRequestBody{
					TokenizedRequest: &fwkrequest.TokenizedRequest{Prompts: []fwkrequest.PromptTokens{{
						MultiModalFeatures: []fwkrequest.MultiModalFeature{
							{Hash: "image-a", Length: 80, Modality: reqcommon.ModalityImage},
							{Hash: "video-a", Length: 20, Modality: reqcommon.ModalityVideo},
							{Hash: "image-a", Length: 160, Modality: reqcommon.ModalityImage},
							{Hash: "audio-a", Length: 5, Modality: reqcommon.ModalityAudio},
						},
					}}},
				},
			}
			endpoints := make([]scheduling.Endpoint, 0, len(tt.candidates)+1)
			for _, pod := range tt.candidates {
				endpoints = append(endpoints, newEndpoint(pod))
			}
			missingMetadata := scheduling.NewEndpoint(nil, &fwkdl.Metrics{}, nil)
			endpoints = append(endpoints, missingMetadata)

			queries := make([]float64, len(items))
			hits := make([][]float64, 2)
			for i, item := range items {
				queries[i] = testutil.ToFloat64(encoderCacheQueriesTotal.WithLabelValues(ProducerType, testName, item.Modality))
			}
			for i, pod := range []k8stypes.NamespacedName{podA, podB} {
				for _, item := range items {
					hits[i] = append(hits[i], testutil.ToFloat64(encoderCacheHitsTotal.WithLabelValues(ProducerType, testName, pod.String(), item.Modality)))
				}
			}
			beforeRatio, err := hitRatioHistogram()
			require.NoError(t, err)

			require.NoError(t, p.Produce(context.Background(), request, endpoints))

			for i, item := range items {
				assert.Equal(t, queries[i]+1, testutil.ToFloat64(encoderCacheQueriesTotal.WithLabelValues(ProducerType, testName, item.Modality)))
			}
			for i, pod := range []k8stypes.NamespacedName{podA, podB} {
				for j, item := range items {
					want := hits[i][j]
					if j == 0 || (i == 0 && j == 1) {
						want++
					}
					assert.Equal(t, want, testutil.ToFloat64(encoderCacheHitsTotal.WithLabelValues(ProducerType, testName, pod.String(), item.Modality)))
				}
			}
			var ratioSum float64
			for i, pod := range tt.candidates {
				var matched []attrmm.MatchItem
				switch pod {
				case podA:
					matched = items[:2]
				case podB:
					matched = items[:1]
				}
				assertMatchInfo(t, p, endpoints[i], matched, items)
				ratioSum += float64(len(matched)) / float64(len(items))
			}
			afterRatio, err := hitRatioHistogram()
			require.NoError(t, err)
			assert.Equal(t, beforeRatio.GetSampleCount()+uint64(len(tt.candidates)), afterRatio.GetSampleCount())
			assert.InDelta(t, beforeRatio.GetSampleSum()+ratioSum, afterRatio.GetSampleSum(), 1e-9)
			_, ok := missingMetadata.Get(p.dk)
			assert.False(t, ok)
			assert.Equal(t, beforeCache, p.cacheSnapshot())
		})
	}
}

func TestProduceConcurrentWithCacheUpdates(t *testing.T) {
	p := newTestProducer(t, nil, nil)
	podA := k8stypes.NamespacedName{Namespace: "default", Name: "pod-a"}
	podB := k8stypes.NamespacedName{Namespace: "default", Name: "pod-b"}
	p.putCacheEntry("image-a", podA)
	items := []attrmm.MatchItem{{Hash: "image-a", Size: 80, Modality: string(reqcommon.ModalityImage)}}

	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Go(func() {
		<-start
		for range 100 {
			p.putCacheEntry("image-a", podB)
			p.removePod(podB.String())
		}
	})
	wg.Go(func() {
		<-start
		for range 100 {
			endpointA, endpointB := newEndpoint(podA), newEndpoint(podB)
			request := requestWithHashes("", map[string]int{"image-a": 80})
			if !assert.NoError(t, p.Produce(context.Background(), request, []scheduling.Endpoint{endpointA, endpointB})) {
				return
			}
			assertMatchInfo(t, p, endpointA, items, items)
			raw, ok := endpointB.Get(p.dk)
			if !assert.True(t, ok) {
				return
			}
			info := raw.(*attrmm.EncoderCacheMatchInfo)
			assert.ElementsMatch(t, items, info.RequestItems())
			for _, item := range info.MatchedItems() {
				assert.Equal(t, items[0], item)
			}
		}
	})
	close(start)
	wg.Wait()

	p.removePod(podB.String())
	endpoint := newEndpoint(podB)
	require.NoError(t, p.Produce(context.Background(), requestWithHashes("", map[string]int{"image-a": 80}), []scheduling.Endpoint{endpoint}))
	assertMatchInfo(t, p, endpoint, nil, items)
	assert.False(t, encoderCacheHitsTotal.DeleteLabelValues(ProducerType, testName, podB.String(), items[0].Modality))
}
