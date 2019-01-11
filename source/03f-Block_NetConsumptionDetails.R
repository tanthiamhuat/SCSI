local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

load(paste0(path,'data/WeeklyNetConsumptionDetails.RData'))

WeeklyNetConsumptionDetails <- as.data.frame(WeeklyNetConsumptionDetails)
WeeklyNetConsumptionDetails_last14weeks <- WeeklyNetConsumptionDetails[,c(1:5,(ncol(WeeklyNetConsumptionDetails)-14):ncol(WeeklyNetConsumptionDetails))]
WeeklyNetConsumptionDetailsCSV <- read.csv(paste0(path,'data/WeeklyNetConsumptionDetails.csv'))

output$block_NetConsumptionDetails_table <- DT::renderDataTable({
  block_selected <- WeeklyNetConsumptionDetails_last14weeks[WeeklyNetConsumptionDetails_last14weeks$block == input$NetConsumptionDetails_Block,]
 
  if('0' %in% input$NetConsumptionDetails_Supply){
    data_filtered <- block_selected[block_selected$supply == "Direct",]
  } else {
    data_filtered <- block_selected[block_selected$supply == "Indirect",]
  }
},options = list(scrollX=TRUE,scrollY='680px',pageLength=115,dom='t',paging=TRUE,ordering=FALSE),rownames = FALSE,escape=FALSE
)

output$downloadWeeklyPunggolConsumptionData <- downloadHandler(
  filename = function() { paste('WeeklyNetConsumptionDetails', '.csv', sep='') },
  content = function(file) {
    write.csv(WeeklyNetConsumptionDetailsCSV, file, row.names = FALSE)
  }
)

output$lastUpdated_NetConsumptionDetails_Weekly <- renderText(
          Updated_DateTime_NetConsumptionDetails
)

