# 读取CSV文件
Figure2 <- read.csv("/Users/sylvia/Desktop/ R BaseCsv_Data Visualization/Figure2.csv")

# 使用ggplot2创建条形图
library(ggplot2)
ggplot(Figure2, aes(x=Image, y=Time, fill=Method)) +
  geom_bar(stat="identity", position=position_dodge(), width=0.5) + 
  geom_text(aes(label=Time), position=position_dodge(width=0.5), vjust=-0.25, size=3.5) +  
  scale_y_log10() +  
  scale_fill_manual(values=c("baseline_python"="lightgrey", "parallel_python"="lightpink", "hadoop_spark"="lightblue","pure_spark"="lightgreen")) + 
  theme_minimal() +
  theme(plot.title = element_text(hjust = 0.5),
        legend.position = c(0.9, 0.9)) +  
  labs(x="An Image in Different Bits", y="Log of Time(s)", fill="Methodology") + 
  ggtitle("Time Cost Using Different Methodologies")