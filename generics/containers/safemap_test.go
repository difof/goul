//

package containers

import (
	"fmt"
	"testing"
	"time"
)

func TestNewSafeMap(t *testing.T) {
	m := NewSafeMap[string, string]()

	funcBuilder := func(id int) {
		key := "key"
		if id%2 == 0 {
			key = fmt.Sprintf("%s%d", key, id/2)
		}

		m.Set(key, "value")
		time.Sleep(time.Millisecond)
		m.GetE(key)
	}

	for i := 0; i < 1000; i++ {
		go funcBuilder(i)
	}

	time.Sleep(time.Second)
}
