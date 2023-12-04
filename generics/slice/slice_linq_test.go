package slice

import (
	"fmt"
	"testing"
)

func TestSliceMap(t *testing.T) {
	type s1 struct{ id int }
	type s2 struct{ id string }
	slice1 := []s1{{1}, {2}, {3}}
	slice2, _ := Map[s1, s2](slice1, func(s s1) (s2, error) {
		return s2{fmt.Sprintf("%d", s.id+1)}, nil
	})
	fmt.Println(slice2)
}

func TestSort(t *testing.T) {
	slice := []int{3, 1, 2}
	Sort(slice)
	if slice[0] != 1 || slice[1] != 2 || slice[2] != 3 {
		t.Fatal(slice)
	}
}

func TestReverse(t *testing.T) {
	slice := []int{1, 2, 3}
	Reverse(slice)
	if slice[0] != 3 || slice[1] != 2 || slice[2] != 1 {
		t.Fatal(slice)
	}
}
