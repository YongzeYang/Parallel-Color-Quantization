# 读取CSV文件
data <- read.csv("/Users/sylvia/Desktop/Variation of the Numerical Value of Iterations N with Respect to K.csv")

ggplot(data, aes(x=K, y=N)) +
  geom_point(colour="yellowgreen") +
  theme_bw() +
  theme(panel.grid.major = element_line(colour = "grey", linetype = "dashed"),
        plot.title = element_text(hjust = 0.5)) +
  labs(x="K", y="N", title="Variation of the Numerical Value of Iterations N with Respect to K")