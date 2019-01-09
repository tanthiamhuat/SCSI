local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

load(paste0(path,'data/ZeroConsumptionCount.RData'))

SuspectedMeters <- ZeroConsumptionCount %>% dplyr::filter(Comments=="Meter is suspected to be blocked.")

load(paste0(path,'data/WeeklyNetConsumptionDetails.RData'))

SuspectedMeters_WeeklyConsumption <- WeeklyNetConsumptionDetails %>% dplyr::filter(meter_sn %in% SuspectedMeters$meter_sn)

WeeklyNetConsumption_BlockSupply <- list()
if (nrow(SuspectedMeters_WeeklyConsumption)>0) {
  for (i in 1:nrow(SuspectedMeters_WeeklyConsumption)) {
      WeeklyNetConsumption_BlockSupply[[i]] <- WeeklyNetConsumptionDetails %>%
                                               dplyr::filter(block %in% SuspectedMeters_WeeklyConsumption[i,]$block &
                                                             supply %in% SuspectedMeters_WeeklyConsumption[i,]$supply &
                                                             meter_sn=="Total" & meter_type=="NET")
  }


  WeeklyNetConsumption_BlockSupply <- rbindlist(WeeklyNetConsumption_BlockSupply) %>% as.data.frame()

  SuspectedMetersWeeklyConsumption <- SuspectedMeters_WeeklyConsumption[,6:ncol(SuspectedMeters_WeeklyConsumption)]
  SuspectedMetersBlockSupply <- WeeklyNetConsumption_BlockSupply[,6:ncol(WeeklyNetConsumption_BlockSupply)]

  SuspectedMetersWeeklyConsumption_long <- gather(SuspectedMetersWeeklyConsumption,WeekNumber,WeeklyConsumption,factor_key=TRUE)
  SuspectedMetersBlockSupply_long <- gather(SuspectedMetersBlockSupply,WeekNumber,NetConsumption,factor_key=TRUE)

  Suspected_Meters <- cbind(SuspectedMetersWeeklyConsumption_long,SuspectedMetersBlockSupply_long[,2])
  colnames(Suspected_Meters)[3] <- "NetConsumption"

  for (i in (nrow(SuspectedMeters)-1):0){
    index <- c(1:nrow(Suspected_Meters))[c(1:nrow(Suspected_Meters))%%3==0]-i
    assign(paste("SuspectedMeter_",SuspectedMeters[nrow(SuspectedMeters)-i,]$meter_sn,sep = ""),
                  Suspected_Meters[index,])
  }
}

output$MeterStop_plot<- renderPlotly({
  SelectedMeter <- SuspectedMeters[SuspectedMeters$meter_sn==input$MetersSuspicion,]$meter_sn
  # https://stat.ethz.ch/pipermail/r-help/2002-May/021823.html
  assign("MeterSuspected",get(paste("SuspectedMeter_",SelectedMeter,sep="")))

  p <- plot_ly(MeterSuspected, x = ~WeekNumber, y = ~WeeklyConsumption, type='scatter',mode='lines',name='WeeklyConsumption',
               marker = list(color = "green"),line = list(color = "green")) %>%
    add_trace(MeterSuspected, x = ~WeekNumber, y = ~NetConsumption, type='scatter',mode='lines',name = 'NetConsumption',
              marker = list(color = "orange"),line = list(color = "orange")) %>%
    layout(xaxis = list(title = 'Weeknumber',tickangle = -17),
           yaxis = list(title = 'Weekly (Net)Consumption'),showlegend = TRUE) %>%
    config(displayModeBar = F)
  p
})

output$MeterStop_block <- renderText({ 
  SelectedMeter_block <- SuspectedMeters[SuspectedMeters$meter_sn==input$MetersSuspicion,]$block
  paste("Block:", SelectedMeter_block)
})

output$MeterStop_supply <- renderText({ 
  SelectedMeter_supply <- SuspectedMeters[SuspectedMeters$meter_sn==input$MetersSuspicion,]$supply
  paste("Supply:", SelectedMeter_supply)
})