# 导入数据
data <- read.csv("/Users/sylvia/Desktop/R Data Base_csv.csv")

# 使用dplyr包处理数据
library(dplyr)
data <- data %>%
  group_by(Different_Colors, Methodology) %>%
  filter(Time_Cost == max(Time_Cost))

# 使用ggplot2包创建折线图
library(ggplot2)
ggplot(data, aes(x=Different_Colors, y=Time_Cost, color=Methodology)) +
  geom_line() +
  geom_point() +
  labs(title="Time Cost for Different Methodologies with Number of Colors Variation", x=" Number of Colors", y="Time Cost(s)") +
  theme_minimal() +
  theme(plot.title = element_text(hjust = 0.5))  # 添加这行代码来使标题居中