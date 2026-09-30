package maat

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	maatv1 "github.com/bootun/maat-go/gen/maat/v1"
)

// fixtureOutput 是共享 fixture 中的期望输出；缺省的字段取零值。
type fixtureOutput struct {
	Kind    string `json:"kind"`
	StepID  string `json:"stepId"`
	Text    string `json:"text"`
	Final   bool   `json:"final"`
	Attempt uint32 `json:"attempt"`
}

// 与控制台、TS SDK 共享的对账用例（后端仓库 test/fixtures/reconcile/，spec §11.5）。
func TestReconcilerSharedFixtures(t *testing.T) {
	files, err := filepath.Glob("testdata/reconcile/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("no fixtures: %v", err)
	}
	for _, f := range files {
		t.Run(strings.TrimSuffix(filepath.Base(f), ".json"), func(t *testing.T) {
			raw, err := os.ReadFile(f) //nolint:gosec // 测试读取仓库内的 fixture
			if err != nil {
				t.Fatal(err)
			}
			var fx struct {
				Description string            `json:"description"`
				Events      []json.RawMessage `json:"events"`
				Outputs     []fixtureOutput   `json:"outputs"`
			}
			if err := json.Unmarshal(raw, &fx); err != nil {
				t.Fatal(err)
			}
			rec := NewReconciler()
			got := []fixtureOutput{}
			for _, e := range fx.Events {
				var ev maatv1.Event
				if err := protojson.Unmarshal(e, &ev); err != nil {
					t.Fatalf("event %s: %v", e, err)
				}
				for _, out := range rec.Apply(&ev) {
					switch o := out.(type) {
					case TextEvent:
						got = append(got, fixtureOutput{Kind: "text", StepID: o.StepID, Text: o.Text, Final: o.Final})
					case StepRewoundEvent:
						got = append(got, fixtureOutput{Kind: "rewound", StepID: o.StepID, Attempt: o.Attempt})
					}
				}
			}
			want := fx.Outputs
			if want == nil {
				want = []fixtureOutput{}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s\n got  %+v\n want %+v", fx.Description, got, want)
			}
		})
	}
}
