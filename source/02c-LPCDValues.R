local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

load(paste0(path,'data/DT/Punggol_WeeklyLPCD.RData'))
load(paste0(path,'data/Weekly_Occupancy_xts.RData'))
load(paste0(path,'data/DailyLPCD.RData'))
load(paste0(path,'data/Daily_Occupancy_xts.RData'))

WeeklyLPCD_Occupancy_xts <- merge(Punggol_WeeklyLPCD_xts,tail(Weekly_Occupancy_xts,nrow(Punggol_WeeklyLPCD_xts)))
WeeklyLPCD_Occupancy_CSV <- data.frame(date=index(Punggol_WeeklyLPCD_xts), coredata(Punggol_WeeklyLPCD_xts))

DailyLPCD_Occupancy_xts <- merge(DailyLPCD_xts,tail(Daily_Occupancy_xts,nrow(DailyLPCD_xts)))
DailyLPCD_Occupancy_CSV <- data.frame(date=index(DailyLPCD_Occupancy_xts), coredata(DailyLPCD_Occupancy_xts))

output$WeeklyLPCD_plot <- renderDygraph({
  graph <- dygraph(WeeklyLPCD_Occupancy_xts, main = "Weekly LPCD and Occupancy") %>%  
    dyRangeSelector() %>%
    dyAxis("y",label=HTML('Weekly LPCD'),valueRange = c(116, 170)) %>%
    dyAxis("y2", label=HTML('Weekly Occupancy (%)'),valueRange = c(85, 100),independentTicks = TRUE) %>%
    dySeries("WeeklyOccupancy", axis = 'y2')
  graph
})

output$downloadWeeklyLPCDData <- downloadHandler(
  filename = function() { paste('WeeklyLPCD_Occupancy', '.csv', sep='') },
  content = function(file) {
    write.csv(WeeklyLPCD_Occupancy_CSV, file, row.names = FALSE)
  }
)

output$lastUpdated_WeeklyUpdate2 <- renderText(Updated_DateTime_WeeklyLPCD)

output$DailyLPCD_plot <- renderDygraph({
  graph <-  dygraph(DailyLPCD_Occupancy_xts,main="Daily LPCD and Occupancy") %>%
    dyRangeSelector() %>%
    dyAxis("y",label=HTML('Daily LPCD'),valueRange = c(110, 170)) %>%
    dyAxis("y2", label=HTML('Daily Occupancy (%)'),valueRange = c(0, 95),independentTicks = TRUE) %>%
    dySeries("DailyOccupancy", axis = 'y2')
  graph
})

output$downloadDailyLPCDData <- downloadHandler(
  filename = function() { paste('DailyLPCD_Occupancy', '.csv', sep='') },
  content = function(file) {
    write.csv(DailyLPCD_Occupancy_CSV, file, row.names = FALSE)
  }
)

output$lastUpdated_DailyLPCD <- renderText(Updated_DateTime_DailyLPCD)
