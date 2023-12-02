library(ggplot2)
library(reshape2)

rescale_func <- function(x) {
  ifelse(x > 3, (x - 3) / 10 + 3, x)
}

df <- read.csv("//Users/sylvia/Desktop/Big Data Group Project Data Visualization/Figure3/ Figure 3.csv")
df_long <- reshape2::melt(df, id.vars = "K", variable.name = "Method", value.name = "Final Size")
df_long$`Final Size` <- log10(df_long$`Final Size`)
df_long$`Final Size` <- sapply(df_long$`Final Size`, rescale_func)
max_y <- max(df_long[df_long$Method == "Spark", ]$`Final Size`)
ggplot(df_long, aes(x = K, y = `Final Size`, color = Method)) +
  geom_line() +
  geom_point() +  
  geom_hline(yintercept = max_y, linetype="dashed", color = "red") + 
  geom_text(aes(label = max_y, x = max(df_long$K)/2, y = max_y), vjust = -0.5, color = "red") +
  scale_color_manual(values = c("Parallel_Python" = "lightgreen", "Hadoop_Spark" = "lightcoral", "Pure_Spark" = "lightblue","Original"="grey")) +
  labs(title = "Image Size after Compression Using Different Methodologies",
       x = "K",
       y = "Log10 of Final Size (kb)",
       color = "Methodology") +
  theme(plot.title = element_text(hjust = 0.5),
        panel.background = element_rect(fill = "white"),
        panel.grid.major = element_line(colour = "grey", linetype = "dashed"),
        panel.grid.minor = element_line(colour = "grey", linetype = "dashed"),
        axis.line = element_line(colour = "black"))
