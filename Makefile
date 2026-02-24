.PHONY: test-py develop-py develop-release-py bench-py

test-py:
	make -C fozziejoin-py/ test

develop-py:
	make -C fozziejoin-py/ develop

develop-release-py:
	make -C fozziejoin-py/ develop-release

bench-py:
	make -C benchmarks/python benchmark

plotbench-rbase:
	make -C benchmarks/r plotbench
