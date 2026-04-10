.PHONY: check test build plotbench check-osbuilds

test:
	Rscript -e "devtools::test()"

build:
	cd builds && R CMD build ../

check:
	cd builds && R CMD check $(FILENAME) --as-cran

check-osbuilds:
	Rscript -e "devtools::check()"
	Rscript -e "devtools::check_win_devel()"
	Rscript -e "devtools::check_win_release()"
	Rscript -e "devtools::check_mac_release()"

plotbench:
	make -C benchmarks/r plotbench

