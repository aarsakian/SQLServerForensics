package page

import (
	"MSSQLParser/utils"
	"strings"
)

type Boot struct {
	Dbi_Version             uint16
	Dbi_CreateVersion       uint16
	Dbi_SEVersion           uint16
	Dbi_dvSplitPoint        utils.LSN
	Dbi_dbbackupLSN         utils.LSN
	Dbi_LastLogBackupTime   [3]byte
	Dbi_nextseqnum          [3]byte
	Dbi_status              [4]byte
	Dbi_crdate              [12]byte  //? 148 bytes
	Dbi_dbname              [256]byte //404
	Unknown                 [4]byte
	Dbi_Dbid                uint16
	Dbi_cmptlevel           uint8
	Dbi_masterfixups        uint8
	Dbi_maxDbTimestamp      uint64
	Dbi_dbbackupLSN2        utils.LSN //424
	Dbi_RebuildLogs         uint16
	Dbi_differentialBaseLSN utils.LSN //432
	Dbi_RestoreFlags        [2]byte
	Dbi_checkptLSN          utils.LSN //444
	Dbi_dbccFlags           uint16
	Dbi_COWLastLSN          utils.LSN
	Dbi_DirtyPageLSN        utils.LSN
	Dbi_RecoveryFlags       [4]byte
	Dbi_lastxact            [4]byte
	Dbi_collation           uint32
	Dbi_relstat             [4]byte
	Dbi_PartitionDbFlags    uint8
	Dbi_familyGUID          [16]byte
	/*	  Dbi_maxLogSpaceUsed	19361792
		  Dbi_recoveryForkNameStack	entry 0	hex (dec)	0x00000035:00007410:0001 (53:29712:1)
		  DbiNFO @0x00000003D2BC6060	Dbi_recoveryForkNameStack	m_guid	e4d74574-bcd6-41ea-89a9-103f408c5339
		  Dbi_recoveryForkNameStack	entry 1	hex (dec)	0x00000035:00007410:0001 (53:29712:1)
		  DbiNFO @0x00000003D2BC6060	Dbi_recoveryForkNameStack	m_guid	d1c7d2c3-43a3-4f58-9c72-54f3d075ed90
		  Dbi_differentialBaseGuid	e647d962-223d-42b4-a38d-e3cd8f928e93
		  Dbi_firstSysIndexes	0001:00000014
		  Dbi_dynamicFileAllocationNewFileID	0
		  Dbi_oldestBackupXactLSN	0:0:0 (0x00000000:00000000:0000)
		  Dbi_lastLogBackupOldestActiveXactLsn	0:0:0 (0x00000000:00000000:0000)
		  Dbi_lastLogBackupCheckpointLsn	0:0:0 (0x00000000:00000000:0000)
		  Dbi_versionChangeLSN	53:26776:88 (0x00000035:00006898:0058)
		  Dbi_mdUpgStat	0x0004
		  Dbi_category	0x0000000000000000
		  Dbi_safetySequence	0
		  Dbi_dbMirrorId	00000000-0000-0000-0000-000000000000
		  Dbi_pageUndoLsn	0:0:0 (0x00000000:00000000:0000)
		  Dbi_pageUndoState	0
		  Dbi_disabledSequence	0
		  Dbi_dbmRedoLsn	0:0:0 (0x00000000:00000000:0000)
		  Dbi_dbmOldestXactLsn	0:0:0 (0x00000000:00000000:0000)
		  Dbi_CloneCpuCount	0
		  Dbi_CloneMemorySize	0
		  Dbi_updSysCatalog	1900-01-01 00:00:00.000
		  Dbi_LogBackupChainOrigin	36:1587:1 (0x00000024:00000633:0001)
		  Dbi_dbccLastKnownGood	1900-01-01 00:00:00.000
		  Dbi_roleSequence	0
		  Dbi_dbmHardenedLsn	0:0:0 (0x00000000:00000000:0000)
		  Dbi_localState	0
		  Dbi_safety	0
		  Dbi_modDate	2023-05-08 12:07:29.053
		  Dbi_verRDB	268439631
		  Dbi_delayedDurabilityOption	0
		  Dbi_transactionFlags	0
		  Dbi_stalePageDetectionOption	0
		  Dbi_inMemorydbOption	0
		  Dbi_svcBrokerGUID	104bd1e5-5729-481c-818b-04c0e3216e68
		  Dbi_svcBrokerOptions	0x00000000
		  Dbi_dbmLogZeroOutstanding	0
		  Dbi_dbmLastGoodRoleSequence	0
		  Dbi_dbmRedoQueue	0
		  Dbi_dbmRedoQueueType	0
		  Dbi_rmidRegistryValueDeleted	0
		  Dbi_dbmConnectionTimeout	0
		  Dbi_AuIdNext	1099511628357
		  Dbi_MinSkipLsn	0:0:0 (0x00000000:00000000:0000)
		  Dbi_commitTsOfcheckptLSN	0
		  Dbi_dbEmptyVersionState	0
		  Dbi_CurrentGeneration	0
		  Dbi_EncryptionHistory	Scan 0	hex (dec)	0x00000000:00000000:0000 (0:0:0)
		  DbiNFO @0x00000003D2BC6060	Dbi_EncryptionHistory	EncryptionScanInfo:ScanId	0
		  Dbi_EncryptionHistory	Scan 1	hex (dec)	0x00000000:00000000:0000 (0:0:0)
		  DbiNFO @0x00000003D2BC6060	Dbi_EncryptionHistory	EncryptionScanInfo:ScanId	0
		  Dbi_EncryptionHistory	Scan 2	hex (dec)	0x00000000:00000000:0000 (0:0:0)
		  DbiNFO @0x00000003D2BC6060	Dbi_EncryptionHistory	EncryptionScanInfo:ScanId	0
		  Dbi_latestVersioningUpgradeLSN	18:65:69 (0x00000012:00000041:0045)
		  Dbi_PendingRestoreOutcomesId	00000000-0000-0000-0000-000000000000
		  Dbi_ContainmentState	0
		  Dbi_hkRecoveryLSN	0:0:0 (0x00000000:00000000:0000)
		  Dbi_hkLogTruncationLSN	0:0:0 (0x00000000:00000000:0000)
		  Dbi_hkCompatibilityMode	0
		  Dbi_hkRootFile	00000000-0000-0000-0000-000000000000
		  Dbi_hkRootFileWatermark	0
		  Dbi_hkTrimLSN	0:0:0 (0x00000000:00000000:0000)
		  Dbi_hkUpgradeLSN	0:0:0 (0x00000000:00000000:0000)
		  Dbi_hkUndeployLSN	0:0:0 (0x00000000:00000000:0000)
		  Dbi_databaseMaxSizeBytes	0
		  Dbi_pvsRowsetIdRegular	72057594042908672
		  Dbi_pvsRowsetIdLongTerm	72057594042974208
		  Dbi_flavorModelDB	0
		  Dbi_verDbOption	0
		  Dbi_logReplicaQuorumHardenedBlock	0
		  Dbi_status_2	0x00000000*/
}

func (boot Boot) GetDBName() string {
	return strings.ReplaceAll(strings.TrimSpace(utils.DecodeUTF16(boot.Dbi_dbname[:])), "\u0000", "")

}
