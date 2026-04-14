.PHONY: check test build plotbench check-osbuilds

PKG := fozziejoin
VERSION := $(shell awk -F': *' '/^Version:/ { print $$2; exit }' DESCRIPTION | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+')
TARBALL := $(PKG)_$(VERSION).tar.gz

test:
	Rscript -e "devtools::test()"

build:
	cd builds && R CMD build ../

check:
	cd builds && R CMD check $(TARBALL) --as-cran

check-osbuilds:
	Rscript -e "devtools::check()"
	Rscript -e "devtools::check_win_devel()"
	Rscript -e "devtools::check_win_release()"
	Rscript -e "devtools::check_mac_release()"

plotbench:
	make -C benchmarks plotbench

