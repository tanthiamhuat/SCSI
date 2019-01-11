local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

load(paste0(path,'data/BlockRoomTypeLPCD.RData'))
lastmonth <- month(today())-1
if (lastmonth==0) {lastmonth=12}
if (lastmonth==1) {
  last2month=12
} else {
  last2month=lastmonth-1
}
PreviousMonth <- format(ISOdate(year(today()), lastmonth, 1), "%B")
Previous2Month <- format(ISOdate(year(today()), last2month, 1), "%B")

thisyear <- year(today())

output$BlockLPCD_NationalAverage_plot1 <- renderPlot({
  title_str <- paste("Block LPCD of Previous 2 Months (",Previous2Month," & ",PreviousMonth,")",sep="")
  LPCD_str <- paste("National Average=",values.LPCD$LPCD,sep="")
  p <-ggplot(data=BlockLPCD, aes(x=block, y=BlockLPCD, fill=Month)) + 
    geom_bar(stat="identity", position=position_dodge()) +
    geom_text(aes(label=round(BlockLPCD)), position=position_dodge(width=0.9),hjust=0.5,vjust=1.1,size=5.0) +
    xlab("Block") +
    ylab("Block LPCD (Litres per Capita per Day)") +
    scale_fill_manual(values=c("#FF7F00", "#00CC00")) + 
    geom_hline(yintercept=values.LPCD$LPCD, color="red")+
    geom_text(aes(3, 143, label=LPCD_str, vjust=-0.81),size=6) + 
    ggtitle(title_str) + theme_grey(base_size = 18) + theme(legend.position = "bottom")
  p
})

output$RoomTypeLPCD_NationalAverage_plot2 <- renderPlot({
  title_str <- paste("Block LPCD of Previous 2 Months (",Previous2Month," & ",PreviousMonth,")",sep="")
  LPCD_str <- paste("National Average=",values.LPCD$LPCD,sep="")
  p <-ggplot(data=RoomTypeLPCD, aes(x=room_type, y=RoomTypeLPCD, fill=Month)) + 
    geom_bar(stat="identity", position=position_dodge()) +
    geom_text(aes(label=round(RoomTypeLPCD)), position=position_dodge(width=0.9),hjust=0.5,vjust=1.1,size=5.0) +
    xlab("RoomType") +
    ylab("RoomType LPCD (Litres per Capita per Day)") +
    scale_fill_manual(values=c("#FF7F00", "#00CC00")) + 
    geom_hline(yintercept=values.LPCD$LPCD, color="red")+
    geom_text(aes(4, 143, label=LPCD_str, vjust=0.0),size=6) + 
    ggtitle(title_str) + theme_grey(base_size = 18) + theme(legend.position = "bottom")
  p
})

output$lastUpdated_BlockNationalAverageLPCD <- renderText({Updated_DateTime_BlockNationalAverageLPCD})