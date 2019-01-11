local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

ConsumptionDownload <- read.fst(paste0(path,'data/DT/ConsumptionDownload.fst'))
load(paste0(path,'data/Updated_DateTime_ConsumptionIndexDownload.RData'))
load(paste0(path,'data/Week.date.RData'))

output$Consumptiondatadownload_customperiod <- renderUI({
  if(input$Consumptiondatadownload_period=='0'){
    dateRangeInput('dateRange',
                   label = h3('Date range'),
                   start = max(ConsumptionDownload$date)-7, end = max(ConsumptionDownload$date),separator = " - ", format = "dd/mm/yyyy",
                   min = begin_date, max=today()
    )
  }else{NULL}
})

start_Consumptiondatadownload <- reactive({
  #this_day <- max(ConsumptionDownload$date) # => should be replaced by Sys.Date()
  this_day <- Sys.Date()
  temp <- switch(input$Consumptiondatadownload_period,
                 "0" = as.Date(as.character(input$dateRange[1]),tz="Asia/Singapore"),
                 "1" = this_day,
                 "2" = this_day-1,
                 "3" = as.Date(Week.date$beg[match(ConsumptionDownload$week[match(this_day,ConsumptionDownload$date)[1]],Week.date$week)]),
                 "4" = as.Date(Week.date$beg[match(ConsumptionDownload$week[match(this_day,ConsumptionDownload$date)[1]],Week.date$week)-1]),
                 "5" = this_day-7,
                 "6" = floor_date(this_day,unit='month'),
                 "7" = floor_date(this_day,unit='month')-months(1), 
                 "8" = this_day-30
                )
  as.character(temp)
})

end_Consumptiondatadownload <- reactive({
  this_day <- max(ConsumptionDownload$date)
  temp <- switch(input$Consumptiondatadownload_period,
                 "0"=as.Date(input$dateRange[2]+1,tz='Asia/Singapore'),
                 "1"=this_day+1,
                 "2"=this_day,
                 "3"=this_day+1,
                 "4"=as.Date(Week.date$beg[match(ConsumptionDownload$week[match(this_day,ConsumptionDownload$date)[1]],Week.date$week)]),
                 "5"=this_day+1,
                 "6"=this_day+1,
                 "7"=floor_date(this_day,unit='month'),
                 "8"=this_day+1
                )
  as.character(temp)
})

filtered_consumption <- reactive({
  ConsumptionDownload %>% dplyr::filter(date >= ymd(start_Consumptiondatadownload()) & date < ymd(end_Consumptiondatadownload())) %>%
                          dplyr::arrange(date) %>% select(-week)
})

output$Consumption_table <-  DT::renderDataTable(tail(filtered_consumption(),100),rownames=FALSE,
                                             options = list(scrollX=FALSE,pageLength=10,bFilter=0,searching=FALSE))
# global search box off (bFilter=0)
# searching = FALSE disables searching, and no searching boxes will be shown;

output$Consumption_table_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <align="justify">
                   This section displays both the index and consumption after computation of the raw index.<br>
                   Index and consumption values are both in litres.<br>
                   As a preview, the above table displays only the last 100 rows of filtered data.<br>
                   Also, take note in the Custom selection, only the last 3 months of data is available.
                   However, Suez recommends to download the data on a monthly basis, or 30 days individually.<br> 
                   In the case of 1200 households, each file would contain less than 1 million rows which is compatible with an Excel spreadsheet.
                   For a file which contains more than 1 million rows, it is advisable to open it with Notepad++.<br>     
                   Take note that meter_type of MAIN refers to Master meters.
                   </td>
                   </tr>
                   </table>'))
  )
})
# download the filtered data, https://yihui.shinyapps.io/DT-info/
# output$downloadConsumptionData <- downloadHandler('consumption-filtered.csv', 
#                                                    content = function(file) {
#                                                    s = input$Consumption_table_rows_all
#                                   write.csv(filtered_consumption()[s, , drop = FALSE], file)
# })

output$downloadConsumptionData <- downloadHandler(
  filename = function() { paste('Consumption', '.csv', sep='') },
  content = function(file) {
    write.csv(filtered_consumption(), file, row.names = FALSE)
  }
)

output$lastUpdated_downloadConsumption <- renderText({Updated_DateTime_ConsumptionIndexDownload})