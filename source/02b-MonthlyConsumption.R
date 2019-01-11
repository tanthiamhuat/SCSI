local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

load(paste0(path,'data/DT/MonthlyConsumption.RData'))  ## include Punggol and Yuhua
load(paste0(path,'data/DT/LPCD_PerMonth.RData')) ## include Punggol and Yuhua
load(paste0(path,'data/DT/TotalHH.RData')) ## include Punggol and Yuhua

PunggolMonthlyConsumptionCSV <- read.csv2(paste0(path,'data/DT/PunggolMonthlyConsumption.csv'),header = TRUE,sep=",")
YuhuaMonthlyConsumptionCSV <- read.csv2(paste0(path,'data/DT/YuhuaMonthlyConsumption.csv'),header = TRUE,sep=",")

PunggolMonthlyLPCDCSV <- read.csv2(paste0(path,'data/DT/PunggolLPCD_PerMonth.csv'),header = TRUE,sep=",")
YuhuaMonthlyLPCDCSV <- read.csv2(paste0(path,'data/DT/YuhuaLPCD_PerMonth.csv'),header = TRUE,sep=",")

output$MonthlyConsumption_plot <- renderPlot({
  if (input$MonthlyCons_site=='0'){
    MonthlyConsumption <- PunggolMonthlyConsumption
    p <- ggplot(data=MonthlyConsumption, aes(x=Month, y=ConsumptionPerMonth/1000, fill=Year)) + 
      geom_bar(stat="identity", position=position_dodge()) +
      geom_text(aes(label=round(ConsumptionPerMonth/1000)), position=position_dodge(width=0.9),hjust=0.5,vjust=1.1,size=5.0) +
      xlab("Month") +
      ylab(bquote('Consumption ('*~ m^3*')')) +
      scale_fill_manual(values=c("#FF7F00", "#00CC00","#FFFF33","#007FFF")) +
      ggtitle("Monthly Consumption") + theme_grey(base_size = 18) + theme(legend.position = "bottom")
    p
  }else{
    MonthlyConsumption <- YuhuaMonthlyConsumption
    p <- ggplot(data=MonthlyConsumption, aes(x=Month, y=ConsumptionPerMonth/1000, fill=Year)) + 
      geom_bar(stat="identity", position=position_dodge()) +
      geom_text(aes(label=round(ConsumptionPerMonth/1000)), position=position_dodge(width=0.9),hjust=0.5,vjust=1.1,size=5.0) +
      xlab("Month") +
      ylab(bquote('Consumption ('*~ m^3*')')) +
      scale_fill_manual(values=c("#FFFF33","#007FFF")) +
      ggtitle("Monthly Consumption") + theme_grey(base_size = 18) + theme(legend.position = "bottom")
    p
  }
})

output$so_MonthlyCons_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <align="justify">Total consumption of only the submeters per month, excluding Child Care Centre.
                   </td>
                   </tr>
                   </table>'))
  )
})  

output$downloadMonthlyConsumptionData <- downloadHandler(
  filename = function(){
    if(input$MonthlyCons_site=='0'){
      paste('PunggolMonthlyConsumption', '.csv', sep='')
    }else{
      paste('YuhuaMonthlyConsumption', '.csv', sep='')
    }
  },
  content = function(file) {
    if(input$MonthlyCons_site=='0'){
      write.csv(PunggolMonthlyConsumptionCSV, file, row.names = FALSE)
    }else{
      write.csv(YuhuaMonthlyConsumptionCSV, file, row.names = FALSE)
    }
  }
)

output$MonthlyLPCD_plot <- renderPlot({
  if (input$MonthlyCons_site=='0'){
    LPCD_PerMonth <- PunggolLPCD_PerMonth
    p <- ggplot(data=LPCD_PerMonth, aes(x=Month, y=AverageLPCD,fill=Year)) +
      geom_bar(stat="identity", position=position_dodge()) +
      geom_text(aes(label=round(AverageLPCD)), position=position_dodge(width=0.9),hjust=0.5,vjust=1.1,size=5.0) +
      xlab("Month") +
      ylab("Average LPCD (Litres Per Capita Per Day)") +
      scale_fill_manual(values=c("#FF7F00", "#00CC00","#FFFF33","#007FFF")) + 
      scale_y_continuous(limits=c(100,max(LPCD_PerMonth$AverageLPCD)+5),oob = rescale_none) +
      ggtitle("Average LPCD") + theme_grey(base_size = 18) + theme(legend.position = "bottom")
    p
  }else{
    LPCD_PerMonth <- YuhuaLPCD_PerMonth
    p <- ggplot(data=LPCD_PerMonth, aes(x=Month, y=AverageLPCD,fill=Year)) +
      geom_bar(stat="identity", position=position_dodge()) +
      geom_text(aes(label=round(AverageLPCD)), position=position_dodge(width=0.9),hjust=0.5,vjust=1.1,size=5.0) +
      xlab("Month") +
      ylab("Average LPCD (Litres Per Capita Per Day)") +
      scale_fill_manual(values=c("#FFFF33","#007FFF")) + 
      scale_y_continuous(limits=c(100,max(LPCD_PerMonth$AverageLPCD)+5),oob = rescale_none) +
      ggtitle("Average LPCD") + theme_grey(base_size = 18) + theme(legend.position = "bottom")
    p
  }
})

output$so_LPCD_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <align="justify">The Average LPCD Per Month is calculated as the Total Consumption for that month, divided by
                                         the Household member (if they declare) or 2016 National Statistics Household Size and 
                                         the total number of days in that month. The Total Household member for each month is displayed
                                         in the below table.
                   </td>
                   </tr>
                   </table>'))
  )
})  

output$downloadMonthlyLPCDData <- downloadHandler(
  filename = function(){
    if(input$MonthlyCons_site=='0'){
      paste('PunggolLPCDPerMonth', '.csv', sep='')
    }else{
      paste('YuhuaLPCDPerMonth', '.csv', sep='')
    }
  },
  content = function(file) {
    if(input$MonthlyCons_site=='0'){
      write.csv(PunggolMonthlyLPCDCSV, file, row.names = FALSE)
    }else{
      write.csv(YuhuaMonthlyLPCDCSV, file, row.names = FALSE)
    }
  }
)

output$TotalHH_table <- DT::renderDataTable(
  if (input$MonthlyCons_site=='0'){
    TotalHH_wide <- PG_TotalHH_wide
  }else{
    TotalHH_wide <- YH_TotalHH_wide
  }
  ,options = list(dom='t', paging=FALSE,ordering=FALSE),rownames = FALSE,
                                            caption = htmltools::tags$caption(
                                            style = 'caption-side: top; text-align: center;',
                                            htmltools::em('Quantity of Residents over time.')
                                            )
)

output$lastUpdated_MonthlyConsumption <- renderText(Updated_DateTime_MonthlyConsumption)