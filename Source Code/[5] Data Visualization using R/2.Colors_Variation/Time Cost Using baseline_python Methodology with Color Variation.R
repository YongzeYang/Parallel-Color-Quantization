# 导入数据
data <- read.csv("/Users/sylvia/Desktop/R Data Base_csv.csv")

# 筛选出baseline_python的数据并保留每个Different_Colors值对应的Time_Cost最大值
library(dplyr)
baseline_data <- data %>%
  filter(Methodology == 'baseline_python') %>%
  group_by(Different_Colors) %>%
  filter(Time_Cost == max(Time_Cost))

# 使用ggplot2包创建条形图
library(ggplot2)
ggplot(baseline_data, aes(x=Different_Colors, y=Time_Cost)) +
  geom_bar(stat="identity", fill="red") +
  geom_line(group=1, linetype="dashed", color="black") +  # 添加这行代码来添加黑色虚线折线
  labs(title="Time Cost Using BASELINE_PYTHON Methodology with Number of Colors Variation", x="Number of Colors", y="Time Cost(s)") +
  theme_minimal() +
  theme(plot.title = element_text(hjust = 0.5))  # 添加这行代码来使标题居中