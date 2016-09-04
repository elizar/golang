package utils

import "testing"

func TestSortMapByValueDefault(t *testing.T) {
	m := map[string]int{"beep": 20, "baap": 17, "boop": 30}
	sm := SortMapByValue(m, 0, false)
	if sm[0].Key != "baap" || sm[1].Key != "beep" || sm[2].Key != "boop" {
		t.Fail()
	}
}

func TestSortMapByValueReverse(t *testing.T) {
	m := map[string]int{"beep": 20, "baap": 17, "boop": 30}
	sm := SortMapByValue(m, 0, true)
	if sm[0].Key != "boop" || sm[1].Key != "beep" || sm[2].Key != "baap" {
		t.Fail()
	}
}

func TestSortMapByValueLimit(t *testing.T) {
	m := map[string]int{"beep": 20, "baap": 17, "boop": 30, "xxx": 21}
	sm := SortMapByValue(m, 3, false)
	if len(sm) != 3 || sm[2].Key == "boop" {
		t.Fail()
	}
}

func TestRoundPositiveNumber(t *testing.T) {
	if Round(11.123) != 11 {
		t.Fail()
	}
	if Round(12.712) != 13 {
		t.Fail()
	}
}

func TestRoundNegativeNumber(t *testing.T) {
	if Round(-11.123) != -11 {
		t.Fail()
	}
	if Round(-12.712) != -13 {
		t.Fail()
	}
}

func TestRoundPlusPositiveNumber(t *testing.T) {
	if RoundPlus(11.172, 1) != 11.2 {
		t.Fail()
	}
	if RoundPlus(12.727, 2) != 12.73 {
		t.Fail()
	}
}
