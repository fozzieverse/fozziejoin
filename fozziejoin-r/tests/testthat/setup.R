options(fozzie.nthread = 2)

# Some tests use the babynames dataset from the babynames R package.
# Acknowledgment: The babynames package is maintained by Hadley Wickham.
# For more details, visit https://cran.r-project.org/package=babynames 
library(babynames)

# Get a subsample so that tests run in reasonable amount of time
baby1 <- head(babynames, 1e3)
baby2 <- tail(babynames, 1e3)
