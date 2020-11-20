package zerosql

type zerosqlResult struct {
	lastInsertID int64
	rowsAffected int64
	err          error
}

func (r zerosqlResult) LastInsertId() (int64, error) {
	return r.lastInsertID, r.err
}

func (r zerosqlResult) RowsAffected() (int64, error) {
	return r.rowsAffected, r.err
}
