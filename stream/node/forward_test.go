package node_test

import (
	"testing"

	"github.com/caravan/streaming/stream/node"
	"github.com/stretchr/testify/assert"
)

func TestForward(t *testing.T) {
	as := assert.New(t)
	node.Forward[int](42, func(i int, err error) {
		as.Equal(42, i)
		as.Nil(err)
	})
}
