package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"os"

	"github.com/segmentio/go-hll"
)

// Quick generator script for the HLL decoder test data: see README.md for instructions.

func getRegisters(h hll.Hll) []byte {
	settings := h.Settings()
	registers := make([]byte, 1<<uint(settings.Log2m))
	for n := range registers {
		registers[n] = h.GetRegister(n)
	}
	return registers
}

func generateHll(output io.Writer, r *rand.Rand, log2m, regwidth int, sparseEnabled bool, count int) {
	h, err := hll.NewHll(hll.Settings{
		Log2m:             log2m,
		Regwidth:          regwidth,
		ExplicitThreshold: 0,
		SparseEnabled:     sparseEnabled,
	})
	if err != nil {
		panic(err)
	}

	for m := 0; m < count; m++ {
		h.AddRaw(r.Uint64())
	}

	serialized := hex.EncodeToString(h.ToBytes())
	hexRegs := hex.EncodeToString(getRegisters(h))
	isSparse := h.IsSparse()
	if _, err := fmt.Fprintf(output, "%s,%d,%d,%t,%t,%s\n", serialized, log2m, regwidth, sparseEnabled, isSparse, hexRegs); err != nil {
		panic(err)
	}
}

func generateHllExplicit(output io.Writer, r *rand.Rand, count int) {
	vals := make([]uint64, 0, count)
	h, err := hll.NewHll(hll.Settings{
		Log2m:             8,
		Regwidth:          6,
		ExplicitThreshold: 31,
		SparseEnabled:     false,
	})
	if err != nil {
		panic(err)
	}

	for m := 0; m < count; m++ {
		val := r.Uint64()
		h.AddRaw(val)
		vals = append(vals, val)
	}

	serialized := hex.EncodeToString(h.ToBytes())
	valsStr := ""
	for i, val := range vals {
		if i > 0 {
			valsStr += "|"
		}
		valsStr += fmt.Sprintf("%d", val)
	}
	if _, err := fmt.Fprintf(output, "%s,%s\n", serialized, valsStr); err != nil {
		panic(err)
	}

}

func writeOutputSparseFull() {
	output, err := os.Create("../hll_test_sparse_full.csv")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := output.Close(); err != nil {
			panic(err)
		}
	}()

	if _, err := fmt.Fprintf(output, "serialized,log2m,regwidth,sparseEnabled,isSparse,hexRegs\n"); err != nil {
		panic(err)
	}

	src := rand.NewPCG(12345, 67890)
	r := rand.New(src)

	for _, log2m := range []int{5, 6, 7, 8, 10} {
		for _, regwidth := range []int{3, 4, 6, 7} {
			for _, sparseEnabled := range []bool{false, true} {
				for _, count := range []int{200, 400, 800, 1600, 3200, 6400, 12800, 25600, 102400} {
					generateHll(output, r, log2m, regwidth, sparseEnabled, count)
				}
			}
		}
	}
}

func writeOutputExplicit() {
	output, err := os.Create("../hll_test_explicit.csv")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := output.Close(); err != nil {
			panic(err)
		}
	}()

	if _, err := fmt.Fprintf(output, "serialized,vals\n"); err != nil {
		panic(err)
	}

	src := rand.NewPCG(23456, 78901)
	r := rand.New(src)

	for _, count := range []int{1, 2, 3, 4, 8, 12} {
		generateHllExplicit(output, r, count)
	}
}

func main() {
	writeOutputSparseFull()
	writeOutputExplicit()
}
