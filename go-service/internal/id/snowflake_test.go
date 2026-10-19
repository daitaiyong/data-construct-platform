package id

import (
	"strconv"
	"testing"
)

func TestGeneratorReturnsUniqueDecimalIDs(t *testing.T) {
	generator, err := NewGenerator(7)
	if err != nil {
		t.Fatal(err)
	}
	first, err := generator.Next()
	if err != nil {
		t.Fatal(err)
	}
	second, err := generator.Next()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("generator returned duplicate IDs")
	}
	if _, err := strconv.ParseInt(first, 10, 64); err != nil {
		t.Fatalf("ID is not a decimal string: %q", first)
	}
}

func TestGeneratorRejectsInvalidNodeID(t *testing.T) {
	if _, err := NewGenerator(maxNodeID + 1); err == nil {
		t.Fatal("expected invalid node ID error")
	}
}

func TestValidRequiresCanonicalPositiveDecimal(t *testing.T) {
	for _, value := range []string{"1", "1700000000000000000"} {
		if !Valid(value) {
			t.Errorf("Valid(%q) = false", value)
		}
	}
	for _, value := range []string{"", "0", "-1", "01", "snowflake"} {
		if Valid(value) {
			t.Errorf("Valid(%q) = true", value)
		}
	}
}
