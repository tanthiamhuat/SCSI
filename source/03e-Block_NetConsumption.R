local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

load(paste0(path,'data/NetConsumption/PunggolWeeklyNetConsumption.RData'))
PunggolWeeklyNetConsumptionCSV <- read.csv2(paste0(path,'data/NetConsumption/PunggolWeeklyNetConsumption.csv'),header = TRUE,sep=",")

load(paste0(path,'data/NetConsumption/MonthlyNetConsumption.RData'))
MonthlyNetConsumptionCSV <- read.csv2(paste0(path,'data/NetConsumption/MonthlyNetConsumption.csv'),header = TRUE,sep=",")

# Select all / Unselect all
observeEvent(input$all_block_netconsumption, {
  if (is.null(input$block_netconsumption)) {
    label.block_netconsumption = sort(unique(X$block))
    updateCheckboxGroupInput(
      session = session, inputId = "block_netconsumption", selected = label.block_netconsumption
    )
  } else {
    updateCheckboxGroupInput(
      session = session, inputId = "block_netconsumption", selected = ""
    )
  }
})

output$checkboxBlock_net <- renderUI({
  label.block_netconsumption = sort(unique(X$block))
  checkboxGroupInput(inputId = "block_netconsumption", label = "Choose", choices = label.block_netconsumption, selected = sort(unique(X$block))[1:2])
})

is.selected.block_net <- reactiveValues(data = NULL)
observeEvent(input$block_netconsumption, {
  is.selected.block_net$data <- input$block_netconsumption
})

output$block_NetConsumption_plot1 <- renderDygraph({
  if(input$NetConsumption_timestep=='0'){
    if(!is.null(is.selected.block_net$data)){
      if(is.null(input$block_netconsumption)){NULL}else{
        selected <- match(input$block_netconsumption,names(PunggolWeeklyNetConsumption_xts))
        graph <- dygraph(PunggolWeeklyNetConsumption_xts[,c(selected)]/1000, main = "Total Net Consumption (Weekly)") %>%  ## block selected
          dyRangeSelector() %>%
          dyAxis("y",label=HTML('Total Net Consumption (m<sup>3</sup>)')) %>%
          dyOptions(colors = c("#e41a1c","#377eb8","#4daf4a","#984ea3","#ff7f00"))
        graph
        }
    }else{
        graph <- dygraph(PunggolWeeklyNetConsumption_xts[,c(1,2)]/1000, main = "Total Net Consumption (Weekly)") %>%  ## first two block selected
          dyRangeSelector() %>%
          dyAxis("y",label=HTML('Total Net Consumption (m<sup>3</sup>)')) %>%
          dyOptions(colors = c("#e41a1c","#377eb8","#4daf4a","#984ea3","#ff7f00"))
        graph
    }
  }
  else if(input$NetConsumption_timestep=='1'){
    if(!is.null(is.selected.block_net$data)){
      if(is.null(input$block_netconsumption)){NULL}else{
        selected <- match(input$block_netconsumption,names(PunggolWeeklyNetConsumption_xts))
        graph <- dygraph(MonthlyNetConsumption_xts[,c(selected)]/1000, main = "Total Net Consumption (Monthly)") %>%
          dyRangeSelector() %>%
          dyAxis("y",label=HTML('Total Net Consumption (m<sup>3</sup>)')) %>%
          dyOptions(colors = c("#e41a1c","#377eb8","#4daf4a","#984ea3","#ff7f00"))
        graph
      }
    }else{
      graph <- dygraph(MonthlyNetConsumption_xts[,c(1,2)]/1000, main = "Total Net Consumption (Monthly)") %>%  ## first two block selected
        dyRangeSelector() %>%
        dyAxis("y",label=HTML('Total Net Consumption (m<sup>3</sup>)')) %>%
        dyOptions(colors = c("#e41a1c","#377eb8","#4daf4a","#984ea3","#ff7f00"))
      graph
    }
  }
})

output$block_NetConsumption_plot2 <- renderDygraph({
  if(input$NetConsumption_timestep=='0'){
    if(!is.null(is.selected.block_net$data)){
      if(is.null(input$block_netconsumption)){NULL}else{
        selected <- match(input$block_netconsumption,names(PunggolWeeklyNetConsumptionRate_xts))
        graph <- dygraph(PunggolWeeklyNetConsumptionRate_xts[,c(selected)], main = "Total Net Consumption Rate (Weekly)") %>%
          dyRangeSelector() %>%
          dyAxis("y",label=HTML('Total Net Consumption Rate (%)')) %>%
          dyOptions(colors = c("#e41a1c","#377eb8","#4daf4a","#984ea3","#ff7f00")) 
        graph
      }
    }else{
      graph <- dygraph(PunggolWeeklyNetConsumptionRate_xts[,c(1,2)]/1000, main = "Total Net Consumption Rate (Weekly)") %>%  ## first two block selected
        dyRangeSelector() %>%
        dyAxis("y",label=HTML('Total Net Consumption Rate (%)')) %>%
        dyOptions(colors = c("#e41a1c","#377eb8","#4daf4a","#984ea3","#ff7f00"))
      graph
    }
  }
  else if(input$NetConsumption_timestep=='1'){
    if(!is.null(is.selected.block_net$data)){
      if(is.null(input$block_netconsumption)){NULL}else{
        selected <- match(input$block_netconsumption,names(PunggolWeeklyNetConsumptionRate_xts))
          graph <- dygraph(MonthlyNetConsumptionRate_xts[,c(selected)], main = "Total Net Consumption Rate (Monthly)") %>%
            dyRangeSelector() %>%
            dyAxis("y",label=HTML('Total Net Consumption Rate (%)')) %>%
            dyOptions(colors = c("#e41a1c","#377eb8","#4daf4a","#984ea3","#ff7f00")) 
          graph
       }
      }else{
        graph <- dygraph(MonthlyNetConsumptionRate_xts[,c(1,2)]/1000, main = "Total Net Consumption Rate (Monthly)") %>%  ## first two block selected
          dyRangeSelector() %>%
          dyAxis("y",label=HTML('Total Net Consumption Rate (%)')) %>%
          dyOptions(colors = c("#e41a1c","#377eb8","#4daf4a","#984ea3","#ff7f00"))
        graph
      }
  }
})

output$block_NetConsumption_table <-  DT::renderDataTable(
  if(input$NetConsumption_timestep=='0'){
    PunggolWeeklyNetConsumption_NA_table
  }
  else if(input$NetConsumption_timestep=='1'){
    MonthlyNetConsumption_NA
  }
  ,filter='bottom',options = list(scrollX=FALSE,pageLength=10)
)

output$downloadNetConsumptionData <- downloadHandler(
  filename = function() {
    if(input$NetConsumption_timestep=='0'){
      paste('PunggolWeeklyNetConsumption', '.csv', sep='')
    }
    else if(input$NetConsumption_timestep=='1'){
      paste('MonthlyNetConsumption', '.csv', sep='')
    }
  },
  content = function(file) {
    if(input$NetConsumption_timestep=='0'){
      write.csv(PunggolWeeklyNetConsumptionCSV, file, row.names = FALSE)
    }
    else if(input$NetConsumption_timestep=='1'){
      write.csv(MonthlyNetConsumptionCSV, file, row.names = FALSE)
    }
  }
)

output$block_NetConsumption_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <align="justify">Date is the last day of WeekNumber referenced from <a href="http://whatweekisit.org/" target="_blank">here.</a><br><br> 
                                         
                   <b>Below is the calculations for Punggol Net Consumption:</b>
                   IndirectNetConsumption=IndirectMasterMeters-IndirectSubMeters.<br>
                   IndirectNetConsumptionRate=(IndirectNetConsumption/IndirectMasterMeters)*100.<br>

                   DirectNetConsumption=DirectMasterMeters-DirectSubMeters.<br>
                   DirectNetConsumptionRate=(DirectNetConsumption/DirectMasterMeters)*100.<br>
                   
                   TotalNetConsumption=IndirectNetConsumption+DirectNetConsumption.<br>
                   TotalMasterMeters=IndirectMasterMeters+DirectMasterMeters.<br> 
                   TotalNetConsumptionRate=(TotalNetConsumption/TotalMasterMeters)*100.<br><br>

                   <b>Below is the calculations for Yuhua Net Consumption:</b>
                   IndirectNetConsumption=DirectMasterMeters-IndirectSubMeters.<br>
                   IndirectNetConsumptionRate=(IndirectNetConsumption/DirectMasterMeters)*100.<br>

                   DirectNetConsumption=sum(DirectSubMeters).<br>
                   DirectNetConsumptionRate=(DirectNetConsumption/DirectMasterMeters)*100.<br>

                   TotalNetConsumption=IndirectNetConsumption+DirectNetConsumption.<br>
                   TotalMasterMeters=DirectMasterMeters.<br> 
                   TotalNetConsumptionRate=(TotalNetConsumption/TotalMasterMeters)*100.

                   <br><br>
                   Take note that the values in the Total Net Consumption plot are in m<sup>3</sup>.<br>
                   Please download the Excel .csv file to see the details of consumption values from Master and Submeters of Direct and Indirect Supply,
                   which the values are in litres. <br><br>

                   Note: Net Consumption SUB meters include all residential SUB meters and child-care centre.

                   </td>
                   </tr>
                   </table>'))
    )
})

output$lastUpdated_NetConsumption <- renderText(
  if(input$NetConsumption_timestep=='0'){
    Updated_DateTime_NetConsumption_Weekly_Punggol
  }
  else if(input$NetConsumption_timestep=='1'){
    Updated_DateTime_NetConsumptionDetails_Monthly
  }
)