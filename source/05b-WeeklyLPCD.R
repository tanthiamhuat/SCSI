local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

load(paste0(path,'data/WeeklyLPCD_OnOffline.RData'))

WeeklyLPCDOnlineOfflineCSV <- read.csv2(paste0(path,'data/WeeklyLPCDOnlineOffline.csv'),header=TRUE,sep=";")

output$CAWeeklyLPCD_plot<- renderPlotly({
  f1 <- list(
    family = "Old Standard TT, serif",
    size = 11,
    color = "black"
  )
  
  p <- plot_ly(WeeklyLPCD_OnOffline, x = ~YearWeek, y = ~WeeklyLPCD_Online, type='scatter',mode='lines',name='Online',
               marker = list(color = "green"),line = list(color = "green")) %>%
    add_trace(WeeklyLPCD_OnOffline, x = ~YearWeek, y = ~WeeklyLPCD_Offline, type='scatter',mode='lines',name = 'Offline',
              marker = list(color = "orange"),line = list(color = "orange")) %>%
    layout(title = 'Weekly LPCD (Online/Offline Customers)',
           xaxis = list(title = 'Year_Weeknumber',tickangle = -24,tickfont = f1),
           yaxis = list(title = 'Weekly LPCD'),showlegend = TRUE) %>%
    config(displayModeBar = F)
  p
})

output$downloadWeeklyLPCDOnlineOfflineData <- downloadHandler(
  filename = function() { paste('WeeklyLPCDOnlineOffline', '.csv', sep='') },
  content = function(file) {
    write.csv(WeeklyLPCDOnlineOfflineCSV, file, row.names = FALSE)
  }
)

output$lastUpdated_WeeklyLPCD <- renderText({Updated_DateTime_WeeklyLPCD})