library(ggplot2)

df <- read.csv("/Users/sylvia/Desktop/ R BaseCsv_Data Visualization/Figure5.csv")
df$Time <- log(df$Time)  
df$Methodology <- reorder(df$Methodology, df$Time) 

ggplot(df, aes(x = Methodology, y = `Time`, fill = `Image`)) +
  geom_bar(stat = "identity", position = "stack", width = 0.5) +
  geom_text(aes(label = round(`Time`, 2)), position = position_stack(vjust = 1.1)) +
  facet_grid(Image ~ .) +
  labs(title = "Total Time Cost for Two Data Sets of Different Data Bits",
       x = "Methodology",
       y = "Log of Time(s)",
       fill = "Image Bits") +
  scale_fill_manual(values = c("16bit RGB" = "lightblue", "8bit RGB" = "lightpink")) +
  theme_minimal() + 
  theme(text = element_text(size = 14),
        plot.margin = margin(1, 1, 1, 1, "cm"))


