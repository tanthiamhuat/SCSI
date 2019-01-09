local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

load(paste0(path,'data/LeakVolumePerDay_Min5Min60.RData'))

LeakVolumePerDayCSV <- read.csv2(paste0(path,'data/LeakVolumePerDay.csv'),header = TRUE,sep=",")

LeakVolumePerDayPerBlockCSV <- read.csv2(paste0(path,'data/LeakVolumePerDayPerBlock.csv'),header = TRUE,sep=",")

output$LeakVolume_plot1 <- renderDygraph({
  graph <- dygraph(LeakVolumePerDay_xts, main = "Leak Volume Per Day (Punggol)") %>%
    dyRangeSelector() %>%
    dyAxis("y",label=HTML('Total Leak Per Day (Litres)')) %>%
    dyOptions(colors = c("#e41a1c","#377eb8"))
  graph
})

output$downloadLeakVolumeData <- downloadHandler(
  filename = function() { paste('LeakVolumePerDay', '.csv', sep='') },
  content = function(file) {
    write.csv(LeakVolumePerDayCSV, file, row.names = FALSE)
  }
)

# Select all / Unselect all
observeEvent(input$all_leakvolume, {
  if (is.null(input$block_leakvolume)) {
    label.block_leakvolume = sort(unique(X$block))
    updateCheckboxGroupInput(
      session = session, inputId = "block_leakvolume", selected = label.block_leakvolume
    )
  } else {
    updateCheckboxGroupInput(
      session = session, inputId = "block_leakvolume", selected = ""
    )
  }
})

output$checkboxBlock_leakvolume <- renderUI({
  label.block_leakvolume = sort(unique(X$block))
  checkboxGroupInput(inputId = "block_leakvolume", label = "Choose", choices = label.block_leakvolume, selected = sort(unique(X$block))[1:2])
})

is.selected.block_leakvolume <- reactiveValues(data = NULL)
observeEvent(input$block_leakvolume, {
  is.selected.block_leakvolume$data <- input$block_leakvolume
})

output$LeakVolume_plot2 <- renderDygraph({
  if(!is.null(is.selected.block_leakvolume$data)){
    if(is.null(input$block_leakvolume)){NULL}else{
      selected <- match(input$block_leakvolume,names(LeakVolumePerDayPerBlock_xts))
      graph <- dygraph(LeakVolumePerDayPerBlock_xts[,c(selected)], main = "Leak Volume Per Block Per Day") %>%
        dyRangeSelector() %>%
        dyAxis("y",label=HTML('Leak Volume (Litres)')) %>%
        dyOptions(colors = c("#e41a1c","#377eb8","#4daf4a","#984ea3","#ff7f00"))
      graph
    }
  }else{
    graph <- dygraph(LeakVolumePerDayPerBlock_xts[,c(1,2)], main = "Leak Volume Per Block Per Day") %>%
      dyRangeSelector() %>%
      dyAxis("y",label=HTML('Leak Volume (Litres)')) %>%
      dyOptions(colors = c("#e41a1c","#377eb8","#4daf4a","#984ea3","#ff7f00"))
    graph
  }
})

output$downloadLeakVolumePerBlockData <- downloadHandler(
  filename = function() { paste('LeakVolumePerDayPerBlockCSV', '.csv', sep='') },
  content = function(file) {
    write.csv(LeakVolumePerDayPerBlockCSV, file, row.names = FALSE)
  }
)

output$LeakVolume_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                  <tr>
                  <td> <align="justify">The above 2 plots consider ONLY the SUB meters, including the Child-Care Centre. Master meters are excluded.<br>
                  </td>
                  </tr>
                  </table>'))
  )
})

output$lastUpdated_LeakVolume <- renderText(Updated_DateTime_LeakVolume)