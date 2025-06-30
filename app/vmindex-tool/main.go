package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
)

var (
	storageDataPath = flag.String("storageDataPath", "vmstorage-data", "Path to storage data")
	tenantNames     = flagutil.NewArrayString("tenant", "Tenant name in the format <accountID>:<projectID> to search for metrics. If empty, all tenants will be processed")
	metricName      = flag.String("metric", "", "Metric name filter for TSIDs. If empty, all TSIDs will be processed")
	partitionName   = flag.String("partition", "", "Partition name to search for metrics. If empty, all partitions will be processed")
)

func parseTenant(tenant string) (uint32, uint32, error) {
	i := strings.Index(tenant, ":")
	if i < 0 {
		return 0, 0, fmt.Errorf("invalid tenant name %q", tenant)
	}
	accountID, err := strconv.ParseUint(tenant[:i], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("cannot parse account ID from tenant %q: %s", tenant, err)
	}
	projectId, err := strconv.ParseUint(tenant[i+1:], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("cannot parse project ID from tenant %q: %s", tenant, err)
	}
	return uint32(accountID), uint32(projectId), nil
}

func main() {
	// Write flags and help message to stdout, since it is easier to grep or pipe.
	flag.CommandLine.SetOutput(os.Stdout)
	envflag.Parse()
	logger.Init()

	// Expose pprof handlers
	listenAddrs := []string{":8080"}
	go httpserver.Serve(listenAddrs, nil, httpserver.ServeOptions{})

	logger.Infof("opening storage at %q", *storageDataPath)
	// Set retention to maximum value to prevent cleaning
	opts := storage.OpenOptions{Retention: math.MaxInt}
	// Switch storage to read-only mode
	//storage.SetFreeDiskSpaceLimit(math.MaxInt)
	strg := storage.MustOpenStorage(*storageDataPath, opts)

	var m storage.Metrics
	strg.UpdateMetrics(&m)
	tm := &m.TableMetrics
	partsCount := tm.SmallPartsCount + tm.BigPartsCount
	blocksCount := tm.SmallBlocksCount + tm.BigBlocksCount
	rowsCount := tm.SmallRowsCount + tm.BigRowsCount
	sizeBytes := tm.SmallSizeBytes + tm.BigSizeBytes
	logger.Infof("successfully opened storage %q; partsCount: %d; blocksCount: %d; rowsCount: %d; sizeBytes: %d",
		*storageDataPath, partsCount, blocksCount, rowsCount, sizeBytes)

	const noDeadline uint64 = 1<<64 - 1

	ptws := strg.GetPartitions()

	metricIdsBuf := make([]uint64, 0)

	for _, ptw := range ptws {
		var totalWithData, totalWithoutData int
		pt := ptw.Partition()

		if *partitionName != "" && ptw.Name() != *partitionName {
			logger.Infof("skipping partition %q", ptw.Name())
			continue
		}
		logger.Infof("processing partition %q", ptw.Name())

		err := pt.ForceCleanAllIndexParts(nil)
		if err != nil {
			panic(err)
		}

		var tenants []string
		if len(*tenantNames) > 0 {
			tenants = *tenantNames
		} else {
			var err error
			tenants, err = pt.SearchTenants(nil, noDeadline)
			if err != nil {
				logger.Fatalf("cannot load tenants: %s", err)
			}
		}

		logger.Infof("found %d tenants", len(tenants))
		sort.Strings(tenants)

		for j, tenant := range tenants {
			if j%10000 == 0 {
				logger.Infof("processed tenants %d/%d: %q", j, len(tenants), tenant)
			}
			accountId, projectId, err := parseTenant(tenant)
			if err != nil {
				logger.Fatalf("cannot parse tenant %q: %s", tenant, err)
			}

			var indexTSIDs []storage.TSID

			if *metricName == "" {
				indexTSIDs, err = pt.SearchTSID(nil, accountId, projectId, noDeadline)
				if err != nil {
					logger.Fatalf("cannot load metrics IDs for tenant %q: %s", tenant, err)
				}
			} else {
				indexTSIDs, err = pt.SearchTSIDByName(nil, accountId, projectId, *metricName, noDeadline)
				if err != nil {
					logger.Fatalf("cannot load metrics IDs with name %q for tenant %q: %s", tenant, *metricName, err)
				}
			}

			metricIds := &uint64set.Set{}
			for _, id := range indexTSIDs {
				metricIds.Add(id.MetricID)
			}

			logger.Infof("tenant %q has %d tsids, %d metrics IDs", tenant, len(indexTSIDs), metricIds.Len())

			sort.Slice(indexTSIDs, func(i, j int) bool { return indexTSIDs[i].Less(&indexTSIDs[j]) })

			//strg.SearchDaysForMetricsIds(nil, accountId, projectId, indexTSIDs)

			metricsIdsWithData, err := pt.SearchMetricIdsWithData(nil, indexTSIDs)
			if err != nil {
				logger.Fatalf("cannot load metrics IDs with data for tenant %q from storage %q: %s", tenant, *storageDataPath, err)
			}

			diff := metricIds.Clone()
			diff.Subtract(metricsIdsWithData)

			pt.DeleteMetricIds(diff.AppendTo(metricIdsBuf[:0]))
			totalWithData += metricsIdsWithData.Len()
			totalWithoutData += diff.Len()

			logger.Infof("tenant %q has %d metrics IDs with data and %d without", tenant, metricsIdsWithData.Len(), diff.Len())
		}
		logger.Infof("partition %q has %d metrics IDs with data and %d without", ptw.Name(), totalWithData, totalWithoutData)

		err = pt.ForceCleanAllIndexParts(nil)
		if err != nil {
			panic(err)
		}
	}

	strg.PutPartitions(ptws)

	strg.MustClose()

	if err := httpserver.Stop(listenAddrs); err != nil {
		logger.Fatalf("cannot stop the webservice: %s", err)
	}
}
