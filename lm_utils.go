package buddystore

import (
	"fmt"
)

func printLogs(opsLog []*OpsLogEntry) {
	fmt.Println("*** LOCK OPERATIONS LOGS ***")
	for i := range opsLog {
		fmt.Println(opsLog[i].OpNum, " | ", opsLog[i].Op, " | ", opsLog[i].Key, " - ", opsLog[i].Version, " | ", opsLog[i].Timeout)
	}
	fmt.Println()
}

// Clears off any stale state before replaying the logs
func clearLMForReplay(lm *LManager) {
	lm.VersionMap = make(map[string]uint)
	lm.RLocks = make(map[string]*RLockEntry)
	lm.WLocks = make(map[string]*WLockEntry)
	lm.currOpNum = 0
	return
}
