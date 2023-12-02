library(ggplot2)
library(reshape2)

df <- read.csv("/Users/sylvia/Desktop/ R BaseCsv_Data Visualization/Figure 1.csv")

df_long <- reshape2::melt(df, id.vars = "K", variable.name = "Method", value.name = "Time")

highlight <- df_long[df_long$K == 128, ]  

highlight2 <- df_long[df_long$K %in% c(48, 64) & df_long$Method == "baseline_python", ]  

ggplot(df_long, aes(x = K, y = Time, color = Method)) +
  geom_line() +
  geom_point() + 
  geom_vline(xintercept = 128, linetype="dashed", color = "black") +  
  geom_point(data = highlight, color = "orange", size = 4) + 
  geom_text(data = highlight, aes(label = Time), vjust = -0.5) +  
  geom_point(data = highlight2, color = "red", size = 4) +  
  theme_minimal() + 
  theme(plot.background = element_rect(fill = "white"), 
        plot.title = element_text(hjust = 0.5)) + 
  scale_y_log10() +
  #
  scale_color_manual(values = c("baseline_python" = "gray", "parallel_python" = "lightgreen", "hadoop_spark" = "lightcoral", "pure_spark" = "lightblue")) +
  labs(title = "Time Cost Using Different Methodologies",
       x = "K",
       y = "Log of Time(s)",
       color = "Methodology")