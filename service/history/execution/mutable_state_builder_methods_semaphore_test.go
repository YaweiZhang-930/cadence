package execution

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/cadence/common/persistence"
)

func testSemaphoreInfo(initiatedID int64, tokenID int) *persistence.SemaphoreInfo {
	return &persistence.SemaphoreInfo{
		Version:         1,
		InitiatedID:     initiatedID,
		SemaphoreName:   "my-semaphore",
		OwnerID:         "3:wid:run-1:7",
		TokenID:         tokenID,
		AcquireDeadline: time.Unix(1700000000, 0).UTC(),
	}
}

func Test__GetSemaphoreInfo(t *testing.T) {
	info := testSemaphoreInfo(7, 0)

	t.Run("hold not found", func(t *testing.T) {
		mb := testMutableStateBuilder(t)
		_, ok := mb.GetSemaphoreInfo(7)
		assert.False(t, ok)
	})
	t.Run("hold found", func(t *testing.T) {
		mb := testMutableStateBuilder(t)
		mb.pendingSemaphoreInfoIDs[7] = info
		result, ok := mb.GetSemaphoreInfo(7)
		assert.True(t, ok)
		assert.Equal(t, info, result)
	})
}

func Test__GetPendingSemaphoreInfos(t *testing.T) {
	mb := testMutableStateBuilder(t)
	assert.Empty(t, mb.GetPendingSemaphoreInfos(), "a fresh run holds nothing")

	info := testSemaphoreInfo(7, 3)
	mb.pendingSemaphoreInfoIDs[7] = info
	assert.Equal(t, map[int64]*persistence.SemaphoreInfo{7: info}, mb.GetPendingSemaphoreInfos())
}

func Test__UpsertSemaphoreInfo(t *testing.T) {
	t.Run("records a new hold", func(t *testing.T) {
		mb := testMutableStateBuilder(t)
		info := testSemaphoreInfo(7, 0)

		mb.UpsertSemaphoreInfo(info)

		assert.Equal(t, map[int64]*persistence.SemaphoreInfo{7: info}, mb.pendingSemaphoreInfoIDs)
		assert.Equal(t, map[int64]*persistence.SemaphoreInfo{7: info},
			mb.updateSemaphoreInfos, "the hold must be queued for the store")
	})

	t.Run("a grant replaces the waiting record under the same id", func(t *testing.T) {
		mb := testMutableStateBuilder(t)
		waiting := testSemaphoreInfo(7, 0)
		granted := testSemaphoreInfo(7, 3)

		mb.UpsertSemaphoreInfo(waiting)
		mb.UpsertSemaphoreInfo(granted)

		assert.Len(t, mb.pendingSemaphoreInfoIDs, 1, "one acquire is one hold, not two")
		assert.Equal(t, granted, mb.pendingSemaphoreInfoIDs[7])
		assert.Equal(t, granted, mb.updateSemaphoreInfos[7])
	})

	t.Run("holds can be recorded after a load left the map nil", func(t *testing.T) {
		// A store that keeps no holds returns nil, and load installs it as-is. Writing to a
		// nil map panics, so the first upsert has to build one.
		mb := testMutableStateBuilder(t)
		mb.pendingSemaphoreInfoIDs = nil
		info := testSemaphoreInfo(7, 3)

		require.NotPanics(t, func() { mb.UpsertSemaphoreInfo(info) })
		assert.Equal(t, info, mb.pendingSemaphoreInfoIDs[7])
	})
}

func Test__DeletePendingSemaphore(t *testing.T) {
	t.Run("drops a hold the run has", func(t *testing.T) {
		mb := testMutableStateBuilder(t)
		mb.UpsertSemaphoreInfo(testSemaphoreInfo(7, 3))

		require.NoError(t, mb.DeletePendingSemaphore(7))

		assert.Empty(t, mb.pendingSemaphoreInfoIDs)
		assert.Empty(t, mb.updateSemaphoreInfos,
			"a hold written and dropped in one transaction must not also be sent as an upsert")
		assert.Equal(t, map[int64]struct{}{7: {}}, mb.deleteSemaphoreInfos)
	})

	t.Run("queues the delete even with nothing in memory", func(t *testing.T) {
		// The record may be in the store but missing here, which is the inconsistency the
		// logging call reports. Queuing the delete anyway is what clears it.
		mb := testMutableStateBuilder(t)

		require.NoError(t, mb.DeletePendingSemaphore(7))

		assert.Equal(t, map[int64]struct{}{7: {}}, mb.deleteSemaphoreInfos)
	})

	t.Run("leaves other holds alone", func(t *testing.T) {
		mb := testMutableStateBuilder(t)
		kept := testSemaphoreInfo(9, 4)
		mb.UpsertSemaphoreInfo(testSemaphoreInfo(7, 3))
		mb.UpsertSemaphoreInfo(kept)

		require.NoError(t, mb.DeletePendingSemaphore(7))

		assert.Equal(t, map[int64]*persistence.SemaphoreInfo{9: kept}, mb.pendingSemaphoreInfoIDs)
		assert.Equal(t, map[int64]*persistence.SemaphoreInfo{9: kept}, mb.updateSemaphoreInfos)
	})
}

// Tests that a run holding a token cannot be reset. The reset would start a new run while the
// hold stayed recorded against the old one, and nothing would give the token back.
func Test__CheckResettableWithSemaphoreHolds(t *testing.T) {
	mb := testMutableStateBuilder(t)
	require.NoError(t, mb.CheckResettable(), "nothing is held yet")

	mb.UpsertSemaphoreInfo(testSemaphoreInfo(7, 3))
	err := mb.CheckResettable()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "pending semaphore holds")
}
