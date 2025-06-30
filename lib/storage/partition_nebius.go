package storage

import (
	"fmt"
	"math"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/querytracer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
)

func (pt *partition) SearchTenants(qt *querytracer.Tracer, deadline uint64) ([]string, error) {
	return pt.idb.SearchTenants(qt, pt.idb.tr, deadline)
}

func (pt *partition) SearchTSID(qt *querytracer.Tracer, accountID, projectID uint32, deadline uint64) ([]TSID, error) {
	return pt.idb.GetTSIDs(qt, accountID, projectID, deadline)
}

func (pt *partition) SearchTSIDByName(qt *querytracer.Tracer, accountID, projectID uint32, name string, deadline uint64) ([]TSID, error) {
	tfs := NewTagFilters(accountID, projectID)
	err := tfs.Add(nil, []byte(name), false, false)
	if err != nil {
		return nil, err
	}

	metricIds, err := pt.idb.searchMetricIDs(qt, []*TagFilters{tfs}, pt.idb.tr, 1e9, deadline)
	if err != nil {
		return nil, fmt.Errorf("cannot search for metric ids for %q: %w", name, err)
	}
	return pt.idb.getTSIDsFromMetricIDs(qt, accountID, projectID, metricIds, deadline)
}

func (pt *partition) SearchDaysForMetricsIds(qt *querytracer.Tracer, accountID, projectID uint32, tsids []TSID) {
	day := fasttime.UnixDate()

	for _, tsid := range tsids {
		var minDay, maxDay uint64 = math.MaxUint64, 0
		for i := range 180 {
			d := day - uint64(180-i)
			if pt.idb.hasDateMetricID(d, tsid.MetricID, accountID, projectID) {
				if d < minDay {
					minDay = d
				}
				if d > maxDay {
					maxDay = d
				}
			}
		}
		minD := time.Unix(int64(minDay)*msecPerDay/1000, 0).Format("2006-01-02")
		maxD := time.Unix(int64(maxDay)*msecPerDay/1000, 0).Format("2006-01-02")
		logger.Infof("%d has [%s, %s]", tsid.MetricID, minD, maxD)
	}
}

// SearchMetricIdsWithData returns metric IDs for the given tsids that have data
// tsids must be sorted.
func (pt *partition) SearchMetricIdsWithData(qt *querytracer.Tracer, tsids []TSID) (*uint64set.Set, error) {
	var ps partitionSearch

	metricIds := &uint64set.Set{}

	i := 0
	ps.Init(pt, tsids, pt.tr)
	for ps.NextBlock() {
		tsid := ps.BlockRef.bh.TSID
		metricIds.Add(tsid.MetricID)
		i++
		if i%1_000_000 == 0 {
			logger.Infof("searching data, processed %d blocks, found %d metric ids", i, metricIds.Len())
		}
	}

	if ps.Error() != nil {
		return nil, ps.Error()
	}
	ps.MustClose()

	return metricIds, nil
}

func (pt *partition) ForceCleanAllIndexParts(stopCh <-chan struct{}) error {
	if pt.idb.getDeletedMetricIDs().Len() > 0 {
		logger.Infof("partition %q has %d deleted metric IDs", pt.name, pt.idb.getDeletedMetricIDs().Len())

		if err := pt.idb.tb.ForceCleanAllParts(stopCh); err != nil {
			return fmt.Errorf("cannot force merge index parts for partition %q: %w", pt.name, err)
		}
	} else {
		logger.Infof("partition %q has 0 deleted metric IDs, skipping force merge", pt.name)
	}

	return nil
}

func (pt *partition) DeleteMetricIds(metricIds []uint64) {
	pt.idb.deleteMetricIDs(metricIds)
}
