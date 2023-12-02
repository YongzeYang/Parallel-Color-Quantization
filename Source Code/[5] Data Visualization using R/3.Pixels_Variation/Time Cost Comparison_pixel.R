# 导入数据
data <- read.csv("/Users/sylvia/Desktop/R Data Base_csv.csv")

# 在每一条曲线x的数值相同时只保留y的最大值
library(dplyr)
data <- data %>%
  group_by(Pixels, Methodology) %>%
  filter(Time_Cost == max(Time_Cost))

# 加载ggplot2包
library(ggplot2)

# 使用ggplot2包创建折线图
ggplot(data, aes(x=Pixels, y=Time_Cost, color=Methodology)) +
  geom_line() +
  geom_point() +
  labs(title="Time Cost for Different Methodologies with Pixel Variation", x="Pixels", y="Time Cost(s)") +
  theme_minimal() +
  theme(plot.title = element_text(hjust = 0.5)) # 将图片标题居中