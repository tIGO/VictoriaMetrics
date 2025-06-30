package storage

func (s *Storage) GetPartitions() []*partitionWrapper {
	return s.tb.GetPartitions(nil)
}

func (s *Storage) PutPartitions(pts []*partitionWrapper) {
	s.tb.PutPartitions(pts)
}
