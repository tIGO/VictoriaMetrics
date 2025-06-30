package mergeset

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

func (tb *Table) getFilePartsForMerge() []*partWrapper {
	var pws []*partWrapper
	tb.partsLock.Lock()
	for _, pw := range tb.fileParts {
		if pw.isInMerge {
			continue
		}
		pw.isInMerge = true
		pws = append(pws, pw)
	}
	tb.partsLock.Unlock()
	return pws
}

// ForceCleanAllParts runs merge for every file part in pt to clean up entries for deleted time series.
func (tb *Table) ForceCleanAllParts(stopCh <-chan struct{}) error {
	pws := tb.getFilePartsForMerge()

	if len(pws) == 0 {
		// Nothing to merge.
		return nil
	}

	wg := getWaitGroup()
	defer putWaitGroup(wg)

	for _, pw := range pws {
		pws := []*partWrapper{pw}
		newPartSize := getPartsSize(pws)
		maxOutBytes := fs.MustGetFreeSpace(tb.path)
		if newPartSize > maxOutBytes {
			freeSpaceNeededBytes := newPartSize - maxOutBytes
			logger.Warnf("cannot initiate force merge for the part %q; additional space needed: %d bytes", pw.p.path, freeSpaceNeededBytes)
			tb.releasePartsToMerge(pws)
			continue
		}

		// Run in parallel to speed up the merge since the current index is global and huge. Probably it is not the best idea for production.
		wg.Add(1)
		filePartsConcurrencyCh <- struct{}{}
		go func(pw *partWrapper) {
			defer func() {
				<-filePartsConcurrencyCh
				wg.Done()
			}()

			err := tb.mergeParts(pws, stopCh, true)
			if err != nil {
				panic(fmt.Errorf("cannot force merge %d part %q: %w", len(pws), pw.p.path, err))
			}
		}(pw)
	}

	wg.Wait()

	return nil
}
