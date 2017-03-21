#R script for graphing top 10 hashtags over time


hashtags <- read.csv("~/Desktop/hashtags/hashtags-20.csv")

hashtags$time <- as.POSIXlt(as.character(hashtags$Timestamp), format="%H:%M:%S")

#find max count for each hashtag
tmpTime <- hashtags$time
hashtags$time <- as.character(hashtags$time)
require(plyr)
cdata <- ddply(hashtags, c("Object"), summarise,
               Max  = max(Count))
hashtags$time <- tmpTime
top10 <- head(arrange(cdata,desc(Max)), 10)
top10Objects <- top10$Object
top10Objects <- as.character(top10Objects)
top10Objects <- as.factor(top10Objects)

#sets objects with counts not in top10 to -1
hashtags$Count <- ifelse(
  hashtags$Object %in% top10Objects,
  hashtags$Count, ## then
  -1 ## else
)

#deletes counts less than 0
hashtags = hashtags[hashtags$Count > 0,]

#plots graph
library("ggplot2")
ggplot(hashtags, aes(x = time, y = Count, group = Object, color=factor(Object))) + geom_line() +
  scale_x_datetime(breaks = ("1 hour")) + xlab("Time") + 
  ylab("Hashtag Counts") + ggtitle("Hashtage Counts in 20 Minute Window") +theme(legend.title=element_blank())




