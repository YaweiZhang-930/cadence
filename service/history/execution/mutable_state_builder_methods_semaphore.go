package execution

import (
	"fmt"

	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
)

// GetSemaphoreInfo returns one hold this run has, whether it holds a token or is still waiting
// for one. The key is the id of the event that started the acquire.
func (e *mutableStateBuilder) GetSemaphoreInfo(
	initiatedEventID int64,
) (*persistence.SemaphoreInfo, bool) {

	si, ok := e.pendingSemaphoreInfoIDs[initiatedEventID]
	return si, ok
}

func (e *mutableStateBuilder) GetPendingSemaphoreInfos() map[int64]*persistence.SemaphoreInfo {
	return e.pendingSemaphoreInfoIDs
}

// UpsertSemaphoreInfo records a hold, replacing whatever sits under the same initiated id.
// One method covers both steps of a hold's life, because TokenID is what separates them: an
// acquire is recorded with no token, and the grant fills one in.
func (e *mutableStateBuilder) UpsertSemaphoreInfo(
	info *persistence.SemaphoreInfo,
) {

	// Load installs whatever the store returned, and a store that keeps no holds returns nil.
	// The update and delete maps are built by the constructor and rebuilt by the flush, so
	// only this one can arrive nil.
	if e.pendingSemaphoreInfoIDs == nil {
		e.pendingSemaphoreInfoIDs = make(map[int64]*persistence.SemaphoreInfo)
	}
	e.pendingSemaphoreInfoIDs[info.InitiatedID] = info
	e.updateSemaphoreInfos[info.InitiatedID] = info
}

// DeletePendingSemaphore drops a hold, which is how a release is recorded. The delete is queued
// for the store even when the entry is missing from memory, so a record the store still has is
// cleared rather than left behind.
func (e *mutableStateBuilder) DeletePendingSemaphore(
	initiatedEventID int64,
) error {

	if _, ok := e.pendingSemaphoreInfoIDs[initiatedEventID]; ok {
		delete(e.pendingSemaphoreInfoIDs, initiatedEventID)
	} else {
		e.logError(
			fmt.Sprintf("unable to find semaphore hold event ID: %v in mutable state", initiatedEventID),
			tag.ErrorTypeInvalidMutableStateAction,
		)
		// log data inconsistency instead of returning an error
		e.logDataInconsistency()
	}

	delete(e.updateSemaphoreInfos, initiatedEventID)
	e.deleteSemaphoreInfos[initiatedEventID] = struct{}{}
	return nil
}
