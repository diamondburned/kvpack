package tests

import (
	"testing"

	"github.com/diamondburned/kvpack"
)

// Suite is the test suite.
type Suite struct {
	db *kvpack.Database
	t  *testing.T
}

// NewSuite creates a new test suite with the given database.
func NewSuite(t *testing.T, db *kvpack.Database) Suite {
	return Suite{db, t}
}

// TestMain runs all tests.
func (s Suite) TestMain() {

}
