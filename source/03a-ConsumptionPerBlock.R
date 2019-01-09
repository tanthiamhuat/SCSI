#X <- Punggol_SUB
X$Date.Time <- X$Date.Time-lubridate::hours(8)

# Select all / Unselect all
observeEvent(input$all_block_consumption, {
  if (is.null(input$block_consumption)) {
    label.blockconsumption = sort(unique(X$block))
    updateCheckboxGroupInput(
      session = session, inputId = "block_consumption", selected = label.blockconsumption
    )
  } else {
    updateCheckboxGroupInput(
      session = session, inputId = "block_consumption", selected = ""
    )
  }
})

output$checkboxBlock_consumption <- renderUI({
  label.blockconsumption = sort(unique(X$block))
  checkboxGroupInput(inputId = "block_consumption", label = "Choose", choices = label.blockconsumption, selected = sort(unique(X$block))[1:2])
})

is.selected.block <- reactiveValues(data = NULL)
observeEvent(input$block_consumption, {
  is.selected.block$data <- input$block_consumption
})

### Block Daily consumption
output$bl_plot_global <- renderDygraph({
  end.date <- max(X$Date)
  begin.date <- as.character(end.date -7)
  end.date <- as.character(end.date)
  if(input$blockconsumption_timestep==0){X$Time_step <- X$Date.Time}else{X$Time_step <- X$Date}
  if(!is.null(is.selected.block$data)){
    if(is.null(input$block_consumption)){NULL}else{
        X <- X %>% filter(block %in% input$block_consumption)
        df_block <- X %>% group_by(block,Time_step) %>% dplyr::summarise(vol=sum(Consumption,na.rm=TRUE)/1000)
        df_block <- as.data.frame(df_block %>% spread(key = block, value = vol))
        Conso_block <- xts(df_block[,-1],df_block$Time_step);
        colnames(Conso_block) <- input$block_consumption
        dygraph(Conso_block) %>% dyRangeSelector(dateWindow=c(begin.date,end.date)) %>% dyLegend(width=500) %>% 
        dyAxis("y",label=HTML('Consumption in m<sup>3</sup>') ,valueRange = c(0, max(Conso_block)*1.1)) %>% 
        dyOptions(useDataTimezone = TRUE) %>%
        dyOptions(colors = c("#e41a1c","#377eb8","#4daf4a","#984ea3","#ff7f00"))
    }
  }else{
    X <- X %>% filter(block %in% sort(unique(X$block))[1:2])
    df_block <- X %>% group_by(block,Time_step) %>% dplyr::summarise(vol=sum(Consumption,na.rm=TRUE)/1000)
    df_block <- as.data.frame(df_block %>% spread(key = block, value = vol))
    Conso_block <- xts(df_block[,-1],df_block$Time_step);
    colnames(Conso_block) <- sort(unique(X$block))[1:2]
    dygraph(Conso_block) %>% dyRangeSelector(dateWindow=c(begin.date,end.date)) %>% dyLegend(width=500) %>% 
    dyAxis("y",label=HTML('Consumption in m<sup>3</sup>') ,valueRange = c(0, max(Conso_block)*1.1)) %>% 
    dyOptions(useDataTimezone = TRUE) %>%
    dyOptions(colors = c("#e41a1c","#377eb8","#4daf4a","#984ea3","#ff7f00"))
  }
})

output$downloadBlockConsumptionData <- downloadHandler(
  filename = function() {
    if(input$blockconsumption_timestep==0){
      paste('HourlyBlockConsumption', '.csv', sep='')
    }
    else if(input$blockconsumption_timestep==1){
      paste('DailyBlockConsumption', '.csv', sep='')
    }
  },
  content = function(file) {
    if(input$blockconsumption_timestep==0){X$Time_step <- X$Date.Time}else{X$Time_step <- X$Date}
    if(!is.null(is.selected.block$data)){
      if(is.null(input$block_consumption)){NULL}else{
        X <- X %>% filter(block %in% input$block_consumption)
        df_block <- X %>% group_by(block,Time_step) %>% dplyr::summarise(vol=sum(Consumption,na.rm=TRUE)/1000)
        df_block <- as.data.frame(df_block %>% spread(key = block, value = vol))
      }
    }else{
      X <- X %>% filter(block %in% sort(unique(X$block))[1:2])
      df_block <- X %>% group_by(block,Time_step) %>% dplyr::summarise(vol=sum(Consumption,na.rm=TRUE)/1000)
      df_block <- as.data.frame(df_block %>% spread(key = block, value = vol))
    }
    write.csv(df_block, file, row.names = FALSE)
  }
)

output$block_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <p align="justify">The plot displays the consumption from the sum of all SUB meters at the particular block. 
                                           None of the MASTER meters are taken into consideration.
                                           Also, the Child-Care Centre is excluded.
                   </p></td>
                   </tr>
                   </table>'))
    )
})

output$lastUpdated_ConsumptionPerBlock <- renderText(Updated_DateTime_HourlyCons)