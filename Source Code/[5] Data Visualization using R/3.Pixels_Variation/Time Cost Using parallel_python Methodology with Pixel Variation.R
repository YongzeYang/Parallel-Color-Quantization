# 导入数据
data <- read.csv("/Users/sylvia/Desktop/R Data Base_csv.csv")

# 筛选出baseline_python的数据
baseline_data <- data[data$Methodology == 'parallel_python',]

# 使用ggplot2包创建条形图
library(ggplot2)
ggplot(baseline_data, aes(x=Pixels, y=Time_Cost)) +
  geom_bar(stat="identity", fill="lightblue") +
  labs(title="Time Cost Using parallel_python Methodology with Pixel Variation", x="Pixels", y="Time Cost(s)") +
  theme_minimal()