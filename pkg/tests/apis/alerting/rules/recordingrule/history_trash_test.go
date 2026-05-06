package recordingrule

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/grafana/grafana/apps/alerting/rules/pkg/apis/alerting/v0alpha1"
	"github.com/grafana/grafana/pkg/apimachinery/utils"
	"github.com/grafana/grafana/pkg/services/featuremgmt"
	ngmodels "github.com/grafana/grafana/pkg/services/ngalert/models"
	"github.com/grafana/grafana/pkg/tests/apis"
	"github.com/grafana/grafana/pkg/tests/apis/alerting/rules/common"
	"github.com/grafana/grafana/pkg/tests/testinfra"
	"github.com/grafana/grafana/pkg/util"
	"github.com/grafana/grafana/pkg/util/testutil"
)

// makeRecordingRuleSpec builds a minimal RecordingRule resource targeting the given folder.
func makeRecordingRuleSpec(t *testing.T, folder, title string) *v0alpha1.RecordingRule {
	t.Helper()
	rule := ngmodels.RuleGen.With(
		ngmodels.RuleMuts.WithUniqueUID(),
		ngmodels.RuleMuts.WithNamespaceUID(folder),
		ngmodels.RuleMuts.WithAllRecordingRules(),
		ngmodels.RuleMuts.WithIntervalMatching(time.Duration(10)*time.Second),
	).Generate()
	return &v0alpha1.RecordingRule{
		ObjectMeta: v1.ObjectMeta{
			Namespace: "default",
			Annotations: map[string]string{
				"grafana.app/folder": folder,
			},
		},
		Spec: v0alpha1.RecordingRuleSpec{
			Title:  title,
			Metric: v0alpha1.RecordingRuleMetricName(rule.Record.Metric),
			Expressions: v0alpha1.RecordingRuleExpressionMap{
				"A": {
					QueryType:     util.Pointer("query"),
					DatasourceUID: util.Pointer(v0alpha1.RecordingRuleDatasourceUID(rule.Data[0].DatasourceUID)),
					Model:         rule.Data[0].Model,
					Source:        util.Pointer(true),
					RelativeTimeRange: &v0alpha1.RecordingRuleRelativeTimeRange{
						From: v0alpha1.RecordingRulePromDurationWMillis("5m"),
						To:   v0alpha1.RecordingRulePromDurationWMillis("0s"),
					},
				},
			},
			Trigger: v0alpha1.RecordingRuleIntervalTrigger{
				Interval: v0alpha1.RecordingRulePromDuration(fmt.Sprintf("%ds", rule.IntervalSeconds)),
			},
		},
	}
}

// TestIntegrationListHistory covers the grafana.app/get-history label selector for RecordingRule.
func TestIntegrationListHistory(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)

	ctx := context.Background()
	helper := common.GetTestHelper(t)
	client := common.NewRecordingRuleClient(t, helper.Org1.Admin)

	common.CreateTestFolder(t, helper, "rec-history-folder")

	resource := makeRecordingRuleSpec(t, "rec-history-folder", "rec-rule-v1")
	created, err := client.Create(ctx, resource, v1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Delete(ctx, created.Name, v1.DeleteOptions{}) })

	for _, title := range []string{"rec-rule-v2", "rec-rule-v3"} {
		latest, err := client.Get(ctx, created.Name, v1.GetOptions{})
		require.NoError(t, err)
		latest.Spec.Title = title
		_, err = client.Update(ctx, latest, v1.UpdateOptions{})
		require.NoError(t, err)
	}

	t.Run("returns version history for recording rules", func(t *testing.T) {
		list, err := client.List(ctx, v1.ListOptions{
			LabelSelector: utils.LabelKeyGetHistory + "=true",
			FieldSelector: "metadata.name=" + created.Name,
		})
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(list.Items), 3, "expected at least 3 versions after two updates")

		titles := make(map[string]struct{}, len(list.Items))
		for _, item := range list.Items {
			titles[item.Spec.Title] = struct{}{}
		}
		assert.Contains(t, titles, "rec-rule-v1")
		assert.Contains(t, titles, "rec-rule-v2")
		assert.Contains(t, titles, "rec-rule-v3")
	})

	t.Run("history listing does not include alerting rules", func(t *testing.T) {
		// Create a separate alerting rule in the same folder. Its versions should NOT be returned
		// when listing history of the recording rule, ensuring rule-type filtering works.
		alertingClient := common.NewAlertRuleClient(t, helper.Org1.Admin)

		alertingRule := ngmodels.RuleGen.With(
			ngmodels.RuleMuts.WithUniqueUID(),
			ngmodels.RuleMuts.WithUniqueTitle(),
			ngmodels.RuleMuts.WithNamespaceUID("rec-history-folder"),
			ngmodels.RuleMuts.WithIntervalMatching(time.Duration(10)*time.Second),
		).Generate()
		alerting := &v0alpha1.AlertRule{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "default",
				Annotations: map[string]string{
					"grafana.app/folder": "rec-history-folder",
				},
			},
			Spec: v0alpha1.AlertRuleSpec{
				Title: alertingRule.Title,
				Expressions: v0alpha1.AlertRuleExpressionMap{
					"A": {
						QueryType:     util.Pointer("query"),
						DatasourceUID: util.Pointer(v0alpha1.AlertRuleDatasourceUID(alertingRule.Data[0].DatasourceUID)),
						Model:         alertingRule.Data[0].Model,
						Source:        util.Pointer(true),
						RelativeTimeRange: &v0alpha1.AlertRuleRelativeTimeRange{
							From: v0alpha1.AlertRulePromDurationWMillis("5m"),
							To:   v0alpha1.AlertRulePromDurationWMillis("0s"),
						},
					},
				},
				Trigger: v0alpha1.AlertRuleIntervalTrigger{
					Interval: v0alpha1.AlertRulePromDuration(fmt.Sprintf("%ds", alertingRule.IntervalSeconds)),
				},
				NoDataState:  v0alpha1.AlertRuleNoDataState(alertingRule.NoDataState),
				ExecErrState: v0alpha1.AlertRuleExecErrState(alertingRule.ExecErrState),
			},
		}
		alertingCreated, err := alertingClient.Create(ctx, alerting, v1.CreateOptions{})
		require.NoError(t, err)
		t.Cleanup(func() { _ = alertingClient.Delete(ctx, alertingCreated.Name, v1.DeleteOptions{}) })

		// Asking for history of the recording rule by name must not include the alerting rule.
		list, err := client.List(ctx, v1.ListOptions{
			LabelSelector: utils.LabelKeyGetHistory + "=true",
			FieldSelector: "metadata.name=" + created.Name,
		})
		require.NoError(t, err)
		for _, item := range list.Items {
			assert.NotEqual(t, alertingCreated.UID, item.UID,
				"alerting rule must not surface through RecordingRule history")
		}
	})
}

// TestIntegrationListTrash covers the grafana.app/get-trash label selector for RecordingRule.
func TestIntegrationListTrash(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)

	ctx := context.Background()
	helper := apis.NewK8sTestHelper(t, testinfra.GrafanaOpts{
		EnableFeatureToggles: []string{
			featuremgmt.FlagAlertRuleRestore,
		},
	})
	client := common.NewRecordingRuleClient(t, helper.Org1.Admin)

	common.CreateTestFolder(t, helper, "rec-trash-folder")

	const (
		liveTitle    = "live-rec-rule-trash-test"
		deletedTitle = "deleted-rec-rule-trash-test"
	)
	live := makeRecordingRuleSpec(t, "rec-trash-folder", liveTitle)
	liveCreated, err := client.Create(ctx, live, v1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Delete(ctx, liveCreated.Name, v1.DeleteOptions{}) })

	deleted := makeRecordingRuleSpec(t, "rec-trash-folder", deletedTitle)
	deletedCreated, err := client.Create(ctx, deleted, v1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, client.Delete(ctx, deletedCreated.Name, v1.DeleteOptions{}))

	// Trash entries change the resource Name to the rule's GUID, so match on Spec.Title instead.
	t.Run("trash listing returns only soft-deleted recording rules", func(t *testing.T) {
		list, err := client.List(ctx, v1.ListOptions{
			LabelSelector: utils.LabelKeyGetTrash + "=true",
		})
		require.NoError(t, err)

		var sawDeleted bool
		for _, item := range list.Items {
			require.NotNil(t, item.DeletionTimestamp,
				"trashed recording rule %q should have a deletion timestamp", item.Name)
			if item.Spec.Title == deletedTitle {
				sawDeleted = true
			}
			assert.NotEqual(t, liveTitle, item.Spec.Title, "live rule must not appear in trash")
		}
		assert.True(t, sawDeleted, "deleted recording rule should appear in trash listing")
	})
}
