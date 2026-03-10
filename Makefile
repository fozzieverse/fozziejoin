RBASEDIR = ./fozziejoin-r

.PHONY: test-py develop-py develop-release-py bench-py check-rbase test-rbase build-rbase plotbench-rbase

test-py:
	make -C fozziejoin-py/ test

develop-py:
	make -C fozziejoin-py/ develop

develop-release-py:
	make -C fozziejoin-py/ develop-release

bench-py:
	make -C benchmarks/python benchmark

test-rbase:
	Rscript -e "devtools::test(pkg = '$(RBASEDIR)')"

build-rbase:
	cd builds && R CMD build ../$(RBASEDIR)

check-rbase: build-rbase
	Rscript -e "devtools::check(pkg = '$(RBASEDIR)')"
	Rscript -e "devtools::check_win_devel('$(RBASEDIR)')"
	Rscript -e "devtools::check_win_release('$(RBASEDIR)')"
	Rscript -e "devtools::check_mac_release('$(RBASEDIR)')"

plotbench-rbase:
	make -C benchmarks/r plotbench

