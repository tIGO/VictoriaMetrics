package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/querytracer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
)

func (db *indexDB) hasDateMetricID(date, metricID uint64, accountID, projectID uint32) bool {
	found := false

	is := db.getIndexSearch(accountID, projectID, noDeadline)
	defer db.putIndexSearch(is)
	found = is.hasDateMetricID(date, metricID, accountID, projectID)
	if found {
		return found
	}

	return found
}

func (db *indexDB) GetTSIDs(qt *querytracer.Tracer, accountID, projectID uint32, deadline uint64) ([]TSID, error) {
	is := db.getIndexSearch(accountID, projectID, deadline)
	defer db.putIndexSearch(is)

	ts := &is.ts
	kb := &is.kb
	kb.B = is.marshalCommonPrefix(kb.B[:0], nsPrefixMetricIDToTSID)
	ts.Seek(kb.B)
	tsids := make([]TSID, 0, 1024)
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, kb.B) {
			break
		}
		v := ts.Item[len(kb.B)+8:]
		tsid := TSID{}
		tail, err := tsid.Unmarshal(v)
		if err != nil {
			logger.Panicf("FATAL: cannot unmarshal the found TSID=%X: %s", v, err)
		}
		if len(tail) > 0 {
			logger.Panicf("FATAL: unexpected non-zero tail left after unmarshaling TSID: %X", tail)
		}
		tsids = append(tsids, tsid)
	}
	if ts.Error() != nil {
		if !errors.Is(ts.Error(), io.EOF) {
			return nil, fmt.Errorf("cannot search for metricIDs in indexDB %q: %w", db.name, ts.Error())
		}
	}

	return tsids, nil
}

func removeDeletedIndexEntries(data []byte, items []mergeset.Item, deletedMetricIDs *uint64set.Set) ([]byte, []mergeset.Item) {
	if deletedMetricIDs.Len() == 0 {
		// No deleted metricIDs, so nothing to do.
		return data, items
	}
	dstData := data[:0]
	dstItems := items[:0]
	for _, it := range items {
		item := it.Bytes(data)
		switch item[0] {
		case nsPrefixMetricIDToTSID, nsPrefixMetricIDToMetricName:
			metricId := encoding.UnmarshalUint64(item[commonPrefixLen:])
			if deletedMetricIDs.Has(metricId) {
				continue
			}
		case nsPrefixDateToMetricID:
			metricId := encoding.UnmarshalUint64(item[commonPrefixLen+8:])
			if deletedMetricIDs.Has(metricId) {
				continue
			}
		case nsPrefixMetricNameToTSID, nsPrefixDateMetricNameToTSID:
			var tsid TSID
			tail, err := tsid.Unmarshal(item[len(item)-marshaledTSIDSize:])
			if err != nil {
				logger.Panicf("FATAL: cannot unmarshal TSID from item %q: %s", item, err)
			}
			if len(tail) > 0 {
				logger.Panicf("FATAL: unexpected tail %q when unmarshaling TSID from item %q", tail, item)
			}
			if deletedMetricIDs.Has(tsid.MetricID) {
				continue
			}
		case nsPrefixTagToMetricIDs, nsPrefixDateTagToMetricIDs:
		case nsPrefixDeletedMetricID:
		default:
			panic(fmt.Sprintf("BUG: unexpected prefix %q", item[0]))
		}

		dstData = append(dstData, item...)
		dstItems = append(dstItems, mergeset.Item{
			Start: uint32(len(dstData) - len(item)),
			End:   uint32(len(dstData)),
		})
	}
	return dstData, dstItems
}
