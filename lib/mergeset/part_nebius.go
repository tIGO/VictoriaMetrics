package mergeset

import (
	"fmt"
)

func (p *part) validateOrder() {
	ps := partSearch{}
	ps.Init(p, true)

	for i := range 256 {
		ps.Seek([]byte{byte(i)})
		hasNext := ps.NextItem()
		if hasNext {
			item := ps.Item
			if item[0] < byte(i) {
				panic(fmt.Sprintf("BUG: search for part %s returns item with first byte (%d) < i (%d), which is not expected", p.path, item[0], i))
			}
		}

		if ps.Error() != nil {
			panic(fmt.Sprintf("BUG: cannot validate order for part %q: %s", p.path, ps.Error()))
		}
	}
}
