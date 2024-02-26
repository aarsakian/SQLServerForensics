package LDF

import "MSSQLParser/utils"

type Manager struct {
	VLFs        *VLFs
	RecordsPtrs []*Record
}

func (manager Manager) DetermineActiveLog() {

	for _, vlf := range *manager.VLFs {
		if vlf.Header.Status != 0 {
			continue
		}
		for _, block := range vlf.Blocks {
			for idx := range block.Records {
				manager.RecordsPtrs = append(manager.RecordsPtrs, &block.Records[idx])
			}

		}
	}

}

func (manager Manager) FilterRecordsByOperation(operationType string) []*Record {
	return utils.Filter(manager.RecordsPtrs, func(record *Record) bool {
		return record.GetOperationType() == operationType
	})
}
