Quick generator script for the HLL decoder test data.

Running this script is not needed because the test data is present in `test-data/hll`, but the script is kept
here for reference and in case modifications to the test data are desired.

To run this:
* Clone go-hll as a subdirectory of this directory: `git clone https://github.com/segmentio/go-hll.git`
* Apply `go-hll-patch.diff`: `cd go-hll && git apply ../go-hll-patch.diff && cd ..`
* Run `go run .`
* This automatically writes `hll_test_sparse_full.csv` and `hll_test_explicit.csv` in the parent directory.
