library(ggplot2)

files <- list(
    "difference" = "../results/rbase_difference_benchmark.csv",
    "distance" = "../results/rbase_distance_benchmark.csv",
    "interval" = "../results/rbase_interval_benchmark.csv",
    "string" = "../results/rbase_string_benchmark.csv"
)

all <- data.frame()
for (file in files) {
    df <- read.csv(file)
    all <- rbind(all, df)
}

forplot <- all[all$method %in% c('difference', 'interval', 'distance', 'cosine'), ]
forplot$time <- forplot$time / 1e9
forplot$n_comps <- forplot$n_comps / 1e6
forplot$n_comps <- factor(sprintf("%.2f", as.numeric(as.character(forplot$n_comps))),
                     levels = unique(sprintf("%.2f", sort(as.numeric(as.character(forplot$n_comps))))))
forplot[forplot$method == 'cosine', 'method'] <- "cosine (string distance)"


myplot <- ggplot(forplot, aes(x = factor(n_comps), y = time, fill = expr)) +
  geom_bar(stat = "summary", fun = mean, position = position_dodge(width = 0.75), width = 0.6) +
  facet_wrap(~ method, scales = "free") +
  labs(
    title = "Benchmarks of fozziejoin vs. fuzzyjoin runtime by select join methods",
    subtitle = "Note: Total comparisons = df1 × df2, but fozziejoin avoids full pairwise evaluation",
    caption = "* Fozziejoin uses optimized algorithms to reduce unnecessary comparisons",
    x = "Total Comparisons (millions)*",
    y = "Time (seconds)",
    fill = "Expression"
  ) +
  theme_minimal(base_size = 14) +
  theme(
    strip.text = element_text(face = "bold", size = 12),
    axis.text.x = element_text(hjust = 1),
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10)),
    legend.position = "bottom",
    legend.title = element_text(face = "bold"),
    plot.title = element_text(face = "bold", size = 16, hjust = 0.5)
  ) +
 scale_fill_brewer(palette = "Set1") +
 scale_y_continuous(labels = scales::label_number(accuracy = 0.1))
ggsave("../results/rbase_bench_plot.png", width = 10, height = 6)
