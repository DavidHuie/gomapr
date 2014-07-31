package gomapr

type Result struct {
	Key   ReduceKey
	Value Partial
}

// A sortable slice of results (sortable only for int64 values).
type ResultSlice []*Result

func (r ResultSlice) Len() int {
	return len(r)
}

func (r ResultSlice) Less(i, j int) bool {
	return r[i].Value.(int64) < r[j].Value.(int64)
}

func (r ResultSlice) Swap(i, j int) {
	tmp := r[i]
	r[i] = r[j]
	r[j] = tmp
}
