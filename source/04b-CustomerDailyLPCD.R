local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

load(paste0(path,'data/CustomerDailyLPCD.RData'))

output$CustomerDailyLPCD_plot <- renderDygraph({
  Family_ID <- as.integer(input$CP_FamilyID)
  Punggol_DailyLPCD <- as.data.frame(Punggol_DailyLPCD)
  Punggol_DailyLPCD_Filtered <- Punggol_DailyLPCD %>% dplyr::filter(FamilyID==Family_ID)
  Punggol_DailyLPCD_Filtered_xts <- xts(Punggol_DailyLPCD_Filtered$DailyLPCD,Punggol_DailyLPCD_Filtered$Date)
  colnames(Punggol_DailyLPCD_Filtered_xts) <- "DailyLPCD"
  
  graph <- dygraph(Punggol_DailyLPCD_Filtered_xts, main = "Customer Daily LPCD") %>%
    dyRangeSelector() %>%
    dyAxis("y",label=HTML('Daily LPDC'))
  graph
})

output$CP_servicepointsn <- renderText({ 
  Family_ID <- as.integer(input$CP_FamilyID)
  Punggol_DailyLPCD <- as.data.frame(Punggol_DailyLPCD)
  Punggol_DailyLPCD_Filtered <- Punggol_DailyLPCD %>% dplyr::filter(FamilyID==Family_ID)
  SelectedServicePointSn <- unique(Punggol_DailyLPCD_Filtered$service_point_sn)
  paste("ServicePointSn:", SelectedServicePointSn)
})

output$lastUpdated_CustomerDailyLPCD <- renderText(Updated_DateTime_CustomerDailyLPCD)