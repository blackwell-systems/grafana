package alertrule

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

// makeAlertRuleSpec builds a minimal AlertRule resource targeting the given folder.
// It uses the model rule generator for plausible expressions/durations so the API server's
// validation accepts the create payload.
func makeAlertRuleSpec(t *testing.T, folder, title string) *v0alpha1.AlertRule {
	t.Helper()
	rule := ngmodels.RuleGen.With(
		ngmodels.RuleMuts.WithUniqueUID(),
		ngmodels.RuleMuts.WithNamespaceUID(folder),
		ngmodels.RuleMuts.WithIntervalMatching(time.Duration(10)*time.Second),
	).Generate()
	return &v0alpha1.AlertRule{
		ObjectMeta: v1.ObjectMeta{
			Namespace: "default",
			Annotations: map[string]string{
				"grafana.app/folder": folder,
			},
		},
		Spec: v0alpha1.AlertRuleSpec{
			Title: title,
			Expressions: v0alpha1.AlertRuleExpressionMap{
				"A": {
					QueryType:     util.Pointer("query"),
					DatasourceUID: util.Pointer(v0alpha1.AlertRuleDatasourceUID(rule.Data[0].DatasourceUID)),
					Model:         rule.Data[0].Model,
					Source:        util.Pointer(true),
					RelativeTimeRange: &v0alpha1.AlertRuleRelativeTimeRange{
						From: v0alpha1.AlertRulePromDurationWMillis("5m"),
						To:   v0alpha1.AlertRulePromDurationWMillis("0s"),
					},
				},
			},
			Trigger: v0alpha1.AlertRuleIntervalTrigger{
				Interval: v0alpha1.AlertRulePromDuration(fmt.Sprintf("%ds", rule.IntervalSeconds)),
			},
			NoDataState:  v0alpha1.AlertRuleNoDataState(rule.NoDataState),
			ExecErrState: v0alpha1.AlertRuleExecErrState(rule.ExecErrState),
		},
	}
}

// TestIntegrationListHistory covers the grafana.app/get-history label selector. The selector is
// the same one unified storage exposes (pkg/storage/unified/apistore/util.go), and legacy storage
// is expected to surface the rule's revision history through the standard list endpoint.
func TestIntegrationListHistory(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)

	ctx := context.Background()
	helper := common.GetTestHelper(t)
	client := common.NewAlertRuleClient(t, helper.Org1.Admin)

	common.CreateTestFolder(t, helper, "history-folder")

	// Create the rule and bump it twice so we have multiple versions to list.
	resource := makeAlertRuleSpec(t, "history-folder", "history-rule-v1")
	created, err := client.Create(ctx, resource, v1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Delete(ctx, created.Name, v1.DeleteOptions{}) })

	// Update twice to grow the version history.
	for _, title := range []string{"history-rule-v2", "history-rule-v3"} {
		latest, err := client.Get(ctx, created.Name, v1.GetOptions{})
		require.NoError(t, err)
		latest.Spec.Title = title
		_, err = client.Update(ctx, latest, v1.UpdateOptions{})
		require.NoError(t, err)
	}

	t.Run("returns all versions for a known rule", func(t *testing.T) {
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
		assert.Contains(t, titles, "history-rule-v1")
		assert.Contains(t, titles, "history-rule-v2")
		assert.Contains(t, titles, "history-rule-v3")

		// Resource version should reflect the rule version on each item, so versions sort distinctly.
		seen := make(map[string]struct{}, len(list.Items))
		for _, item := range list.Items {
			seen[item.ResourceVersion] = struct{}{}
		}
		assert.GreaterOrEqual(t, len(seen), 3, "history items should have distinct resource versions")
	})

	t.Run("rejects history requests without metadata.name field selector", func(t *testing.T) {
		_, err := client.List(ctx, v1.ListOptions{
			LabelSelector: utils.LabelKeyGetHistory + "=true",
		})
		require.Error(t, err, "history listing must require metadata.name")
	})

	t.Run("returns 404 for unknown rule", func(t *testing.T) {
		_, err := client.List(ctx, v1.ListOptions{
			LabelSelector: utils.LabelKeyGetHistory + "=true",
			FieldSelector: "metadata.name=does-not-exist",
		})
		require.Error(t, err)
	})

	t.Run("rejects mixing the history label with other label requirements", func(t *testing.T) {
		_, err := client.List(ctx, v1.ListOptions{
			LabelSelector: utils.LabelKeyGetHistory + "=true,grafana.app/folder=history-folder",
			FieldSelector: "metadata.name=" + created.Name,
		})
		require.Error(t, err)
	})
}

// TestIntegrationListTrash covers the grafana.app/get-trash label selector. With the
// alertRuleRestore feature flag enabled, deleting a rule keeps a tombstone version that
// must surface through the trash listing.
func TestIntegrationListTrash(t *testing.T) {
	testutil.SkipIntegrationTestInShortMode(t)

	ctx := context.Background()
	helper := apis.NewK8sTestHelper(t, testinfra.GrafanaOpts{
		EnableFeatureToggles: []string{
			featuremgmt.FlagAlertRuleRestore,
		},
	})
	client := common.NewAlertRuleClient(t, helper.Org1.Admin)

	common.CreateTestFolder(t, helper, "trash-folder")

	const (
		liveTitle    = "live-rule-trash-test"
		deletedTitle = "deleted-rule-trash-test"
	)
	live := makeAlertRuleSpec(t, "trash-folder", liveTitle)
	liveCreated, err := client.Create(ctx, live, v1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Delete(ctx, liveCreated.Name, v1.DeleteOptions{}) })

	deleted := makeAlertRuleSpec(t, "trash-folder", deletedTitle)
	deletedCreated, err := client.Create(ctx, deleted, v1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, client.Delete(ctx, deletedCreated.Name, v1.DeleteOptions{}))

	// Trash entries change the resource Name to the rule's GUID (the original UID is
	// cleared during soft delete). Match on Spec.Title instead, which we control end-to-end.
	t.Run("trash includes deleted rules but not live ones", func(t *testing.T) {
		list, err := client.List(ctx, v1.ListOptions{
			LabelSelector: utils.LabelKeyGetTrash + "=true",
		})
		require.NoError(t, err)

		var sawDeleted bool
		for _, item := range list.Items {
			if item.Spec.Title == deletedTitle {
				sawDeleted = true
			}
			assert.NotEqual(t, liveTitle, item.Spec.Title, "live rule must not appear in trash")
		}
		assert.True(t, sawDeleted, "deleted rule should appear in trash listing")
	})

	t.Run("trash items carry a deletion timestamp", func(t *testing.T) {
		list, err := client.List(ctx, v1.ListOptions{
			LabelSelector: utils.LabelKeyGetTrash + "=true",
		})
		require.NoError(t, err)
		require.NotEmpty(t, list.Items, "expected at least one trashed rule")
		for _, item := range list.Items {
			assert.NotNil(t, item.DeletionTimestamp, "trashed rule %q should have a deletion timestamp", item.Name)
		}
	})

	t.Run("rejects non-true value for the trash label", func(t *testing.T) {
		_, err := client.List(ctx, v1.ListOptions{
			LabelSelector: utils.LabelKeyGetTrash + "=false",
		})
		require.Error(t, err)
	})
}
