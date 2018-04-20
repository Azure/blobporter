package util

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrintSizes(t *testing.T) {
	var size uint64 = 12
	psize := "12B (12)"

	//bytes scenario
	assert.Equal(t, psize, PrintSize(size))

	//zero scenario
	size = 0
	psize = "0B (0)"
	assert.Equal(t, psize, PrintSize(size))

	//KB scenario
	size = KB + 1
	psize = fmt.Sprintf("1KiB (%v)", KB+1)
	assert.Equal(t, psize, PrintSize(size))

	//MB scenario
	size = MB + 1
	psize = fmt.Sprintf("1MiB (%v)", MB+1)
	assert.Equal(t, psize, PrintSize(size))

	//GB scenario
	size = GB + 1
	psize = fmt.Sprintf("1GiB (%v)", GB+1)
	assert.Equal(t, psize, PrintSize(size))

	//KB scenario (exact)
	size = KB
	psize = fmt.Sprintf("1KiB (%v)", KB)
	assert.Equal(t, psize, PrintSize(size))

	//MB scenario (exact)
	size = MB
	psize = fmt.Sprintf("1MiB (%v)", MB)
	assert.Equal(t, psize, PrintSize(size))

	//GB scenario (exact)
	size = GB
	psize = fmt.Sprintf("1GiB (%v)", GB)
	assert.Equal(t, psize, PrintSize(size))
}
