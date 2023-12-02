# 导入数据
data <- read.csv("/Users/sylvia/Desktop/R Data Base_csv.csv")

# 筛选出baseline_python的数据
baseline_data <- data[data$Methodology == 'hadoop_spark',]

# 在每一条曲线x的数值相同时只保留y的最大值
library(dplyr)
baseline_data <- baseline_data %>%
  group_by(Different_Colors) %>%
  filter(Time_Cost == max(Time_Cost))

# 使用ggplot2包创建条形图
library(ggplot2)
ggplot(baseline_data, aes(x=Different_Colors, y=Time_Cost)) +
  geom_bar(stat="identity", fill="yellowgreen") +
  geom_line(aes(group = 1), color = "black", linetype = "dashed") +
  labs(title="Time Cost Using HADOOP_SPARK Methodology with Number of Colors Variation", x="Number of Colors ", y="Time Cost(s)") +
  theme_minimal() +
  theme(plot.title = element_text(hjust = 0.5)) # 将图片标题居中