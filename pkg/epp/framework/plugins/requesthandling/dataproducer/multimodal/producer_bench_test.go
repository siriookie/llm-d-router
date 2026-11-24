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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	fwkrequest "github.com/llm-d/llm-d-router/pkg/epp/framework/common/request"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrmm "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/multimodal"
)

func BenchmarkProduce(b *testing.B) {
	for _, tc := range []struct {
		pods       int
		candidates int
		items      int
		hitPercent int
	}{
		{pods: 4, candidates: 4, items: 4, hitPercent: 50},
		{pods: 64, candidates: 64, items: 16, hitPercent: 0},
		{pods: 64, candidates: 64, items: 16, hitPercent: 50},
		{pods: 64, candidates: 64, items: 16, hitPercent: 100},
		{pods: 256, candidates: 256, items: 64, hitPercent: 50},
		{pods: 256, candidates: 4, items: 64, hitPercent: 50},
		{pods: 64, candidates: 0, items: 16, hitPercent: 50},
	} {
		b.Run(fmt.Sprintf("pods=%d/candidates=%d/items=%d/hit=%d", tc.pods, tc.candidates, tc.items, tc.hitPercent), func(b *testing.B) {
			p, err := New(b.Context(), b.Name(), nil, nil)
			require.NoError(b, err)
			features := make([]fwkrequest.MultiModalFeature, tc.items)
			for i := range features {
				features[i] = fwkrequest.MultiModalFeature{
					Hash: fmt.Sprintf("hash-%d", i), Length: 16, Modality: reqcommon.ModalityImage,
				}
			}
			endpoints := make([]scheduling.Endpoint, 0, tc.candidates)
			for i := range tc.pods {
				pod := k8stypes.NamespacedName{Namespace: "default", Name: fmt.Sprintf("pod-%d", i)}
				cache := p.getOrCreatePodCache(pod.String())
				for j, feature := range features {
					if tc.hitPercent == 100 || (tc.hitPercent == 50 && (i+j)%2 == 0) {
						cache.commit(attrmm.MatchItem{Hash: feature.Hash, Size: 1})
					}
				}
				if i < tc.candidates {
					endpoints = append(endpoints, newEndpoint(pod))
				}
			}
			request := &scheduling.InferenceRequest{
				Body: &fwkrequest.InferenceRequestBody{
					TokenizedRequest: &fwkrequest.TokenizedRequest{Prompts: []fwkrequest.PromptTokens{{MultiModalFeatures: features}}},
				},
			}
			require.NoError(b, p.Produce(b.Context(), request, endpoints))
			b.ReportAllocs()
			for b.Loop() {
				if err := p.Produce(b.Context(), request, endpoints); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
