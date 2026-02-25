SUBDIR = ./fozziejoin-r

.PHONY: test-py develop-py develop-release-py bench-py

test-py:
	make -C fozziejoin-py/ test

develop-py:
	make -C fozziejoin-py/ develop

develop-release-py:
	make -C fozziejoin-py/ develop-release

bench-py:
	make -C benchmarks/python benchmark

check-rbase:
	Rscript -e "devtools::check(pkg = '$(SUBDIR)')"

test-rbase:
	Rscript -e "devtools::test(pkg = '$(SUBDIR)')"

build-rbase:
	R CMD build ./fozziejoin-r

check-osbuilds-rbase:
	Rscript -e "devtools::check_win_devel('./fozziejoin-r')"
	Rscript -e "devtools::check_win_release('./fozziejoin-r')"
	Rscript -e "devtools::check_mac_release('./fozziejoin-r')"

plotbench-rbase:
	make -C benchmarks/r plotbench
