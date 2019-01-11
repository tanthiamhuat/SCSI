local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

load(paste0(path,'data/OnlineLPCDPerFamilySize.RData'))

Online_LPCDPerFamilySizeCSV <- read.csv2(paste0(path,'data/HHSize_AverageOnlineLPCD.csv'),header=TRUE,sep=";")

output$CAOnlineLPCDPerFamilySize_plot<- renderPlotly({
  p <- plot_ly(
    x = HHSize_AverageOnlineLPCD$num_house_member,
    y = HHSize_AverageOnlineLPCD$AverageLPCD,
    type='bar',
    marker = list(color = 'green'),
    name = 'Avg Online LPCD'
  ) %>%
  add_trace(
    p,
    x = HHSize_AverageOnlineLPCD$num_house_member,
    y = HHSize_AverageOnlineLPCD$AverageLPCD,
    type='scatter',
    mode='lines',
    line = list(color = 'black'),
    name = 'Trendline'
  ) %>%
    layout(yaxis = list(title = 'Average Online LPCD'), barmode = 'group') %>%
    layout(xaxis = list(title = 'Number of Household Members')) %>%
    config(displayModeBar = F)
  p
})

output$downloadOnlineLPCDPerFamilySizeData <- downloadHandler(
  filename = function() { paste('Online_LPCDPerFamilySize', '.csv', sep='') },
  content = function(file) {
    write.csv(Online_LPCDPerFamilySizeCSV, file, row.names = FALSE)
  }
)

output$lastUpdated_OnlineLPCDPerFamilySize <- renderText({Updated_DateTime_OnlineLPCDPerFamilySize})