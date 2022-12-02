package generics

import (
	"fmt"
	"testing"
)

func TestSliceMap(t *testing.T) {
	type s1 struct{ id int }
	type s2 struct{ id string }
	slice1 := []s1{{1}, {2}, {3}}
	slice2 := SliceMap[s1, s2](slice1, func(s s1) s2 { return s2{fmt.Sprintf("%d", s.id+1)} })
	fmt.Println(slice2)
}
