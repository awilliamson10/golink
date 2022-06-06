package utils

import (
	"log"
	"reflect"
	"sort"
	"time"
)

func Type(v interface{}) reflect.Type {
	return reflect.TypeOf(v)
}

func InList[v comparable](value v, list []v) bool {
	for _, item := range list {
		if value == item {
			return true
		}
	}
	return false
}

func GetKeys[v string | float64 | int](m map[string]v) []string {
	keys := []string{}
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func GetValues[v string | float64 | int](m map[string]v) []v {
	values := []v{}
	for _, value := range m {
		values = append(values, value)
	}
	return values
}

func CountOccurrences[v comparable](value v, list []v) int {
	count := 0
	for _, item := range list {
		if value == item {
			count++
		}
	}
	return count
}

func Filter[v comparable](arr []v, cond func(v) bool) []v {
	var res []v
	for _, val := range arr {
		if cond(val) {
			res = append(res, val)
		}
	}
	return res
}

func TimeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func RemoveDuplicates[v comparable](list []v) (res []v) {
	if len(list) < 1 {
		return res
	}
	prev := 1
	for curr := 1; curr < len(list); curr++ {
		if list[curr-1] != list[curr] {
			list[prev] = list[curr]
			prev++
		}
	}
	res = list[:prev]
	return
}

func Slices(l int, remove []int) (res [][]int) {
	sort.Ints(remove)
	remove = RemoveDuplicates(remove)
	start := 0
	for _, r := range remove {
		if r == start {
			start++
			continue
		}
		res = append(res, []int{start, r})
		start = r + 1
	}
	if start < l {
		res = append(res, []int{start, l})
	}
	if start == l {
		res = append(res, []int{start, start})
	}
	return
}
