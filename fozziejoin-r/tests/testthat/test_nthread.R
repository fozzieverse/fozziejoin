test_that("multi-column left difference join matches rows across multiple keys", {
  set.seed(42)
  tsize <- 1e4
  df1 <- data.frame(
    x = rep(seq(1.0, tsize, by = 1.0), each = 10),
    y = rnorm(tsize, mean = 50, sd = 10)
  )

  df2 <- data.frame(
    x = seq(1.0, tsize, by = 1.0) + runif(10, -0.2, 0.2),
    y = rnorm(tsize, mean = 55, sd = 10)
  )

  # Default should be 2 in testing normally
  runtime <- system.time(fozzie_difference_left_join(
        df1, df2, by = c(x = "x", y = "y"), max_distance = 0.15
  ))
  testthat::expect_lte(runtime["user.self"], 2.0 * runtime["elapsed"])

  # We should still be able to force it to be 1 thread
  runtime2 <- system.time(fozzie_difference_left_join(
        df1, df2, by = c(x = "x", y = "y"), max_distance = 0.15, nthread = 1
  ))
  testthat::expect_lt(runtime2["user.self"], 2.1 * runtime2["elapsed"])
})
