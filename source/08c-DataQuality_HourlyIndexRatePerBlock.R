local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = local_path

load(paste0(path,'data/DailyHourlyIndexRate_last30days.RData'))

blocks_PG <- sort(unique(HourlyIndexReadingRate_PunggolBlocks$block))

for (i in 1:length(blocks_PG)){
  assign(paste("HourlyIndexReadingRate_",blocks_PG[i],sep=""), HourlyIndexReadingRate_PunggolBlocks %>% dplyr::filter(block==blocks_PG[i]))
}

HourlyIndexReadingRate_PunggolBlocksDate <-HourlyIndexReadingRate_PunggolBlocks %>% dplyr::select_("date") %>% unique()

HourlyIndexReadingRate_PunggolBlockList <- list()
for(i in 1:length(blocks_PG)){
  HourlyIndexReadingRate_PunggolBlockList[[i]] <- HourlyIndexReadingRate_PunggolBlocks %>% dplyr::filter(block==blocks_PG[i]) %>% 
                                                  dplyr::select_("HourlyIndexReadingRate")
}

HourlyIndexRate_PunggolBlocks <- as.data.frame(unlist(HourlyIndexReadingRate_PunggolBlockList))
nr <- nrow(HourlyIndexReadingRate_PunggolBlocks)
n <- nr/length(blocks_PG)
HourlyIndexRate_PunggolBlocks <- as.data.frame(split(HourlyIndexRate_PunggolBlocks, rep(1:ceiling(nr/n), each=n, length.out=nr)))
colnames(HourlyIndexRate_PunggolBlocks) <- blocks_PG

## if there is no Date till yesterday, fill it with zeros.
# yesterday <- today()-1
# if (max(HourlyIndexReadingRate_BlockDate$date) < yesterday){
#   add_dates <- seq.Date(max(HourlyIndexReadingRate_BlockDate$date)+1,yesterday,by="days") %>% as.data.frame()
#   colnames(add_dates) <- "date"
#   HourlyIndexReadingRate_BlockDate <- rbind(HourlyIndexReadingRate_BlockDate,add_dates)
#   HourlyIndexReadingRate_Block_add <-data.frame(matrix(0,nrow = nrow(add_dates),ncol=ncol(HourlyIndexReadingRate_Block)))
#   colnames(HourlyIndexReadingRate_Block_add) <- names(HourlyIndexReadingRate_Block)
#   HourlyIndexReadingRate_Block <- rbind(HourlyIndexReadingRate_Block,HourlyIndexReadingRate_Block_add)
# }

if (nrow(HourlyIndexRate_PunggolBlocks)==nrow(HourlyIndexReadingRate_PunggolBlocksDate)){
  HourlyIndexRate_PunggolBlocks_xts <- xts(HourlyIndexRate_PunggolBlocks,order.by=as.Date(HourlyIndexReadingRate_PunggolBlocksDate$date))
}

HourlyIndexRatePerPunggolBlock <- as.data.table(HourlyIndexRate_PunggolBlocks_xts)
colnames(HourlyIndexRatePerPunggolBlock)[1] <- "Date"

### Yuhua Blocks
blocks_YH <- sort(unique(HourlyIndexReadingRate_YuhuaBlocks$block))

for (i in 1:length(blocks_YH)){
  assign(paste("HourlyIndexReadingRate_",blocks_YH[i],sep=""), HourlyIndexReadingRate_YuhuaBlocks %>% dplyr::filter(block==blocks_YH[i]))
}

HourlyIndexReadingRate_YuhuaBlocksDate <-HourlyIndexReadingRate_YuhuaBlocks %>% dplyr::select_("date") %>% unique()

HourlyIndexReadingRate_YuhuaBlockList <- list()
for(i in 1:length(blocks_YH)){
  HourlyIndexReadingRate_YuhuaBlockList[[i]] <- HourlyIndexReadingRate_YuhuaBlocks %>% dplyr::filter(block==blocks_YH[i]) %>% 
    dplyr::select_("HourlyIndexReadingRate")
}

HourlyIndexRate_YuhuaBlocks <- as.data.frame(unlist(HourlyIndexReadingRate_YuhuaBlockList))
nr <- nrow(HourlyIndexReadingRate_YuhuaBlocks)
n <- nr/length(blocks_YH)
HourlyIndexRate_YuhuaBlocks <- as.data.frame(split(HourlyIndexRate_YuhuaBlocks, rep(1:ceiling(nr/n), each=n, length.out=nr)))
colnames(HourlyIndexRate_YuhuaBlocks) <- blocks_YH

#min_row <- min(nrow(HourlyIndexRate_YuhuaBlocks),nrow(HourlyIndexReadingRate_YuhuaBlocksDate))

# HourlyIndexRate_YuhuaBlocks_xts <- xts(HourlyIndexRate_YuhuaBlocks,order.by=as.Date(HourlyIndexReadingRate_YuhuaBlocksDate$date)[1:nrow(HourlyIndexRate_YuhuaBlocks)])
## above line is only temporary, as there are inconsistent number of rows of each of Yuhua blocks

HourlyIndexRate_YuhuaBlocks_xts <- xts(HourlyIndexRate_YuhuaBlocks,order.by=as.Date(HourlyIndexReadingRate_YuhuaBlocksDate$date))

HourlyIndexRatePerYuhuaBlock <- as.data.table(HourlyIndexRate_YuhuaBlocks_xts)
colnames(HourlyIndexRatePerYuhuaBlock)[1] <- "Date"

HourlyIndexRatePerPunggolYuhuaBlock <- cbind(HourlyIndexRatePerPunggolBlock,HourlyIndexRatePerYuhuaBlock[,2:ncol(HourlyIndexRatePerYuhuaBlock)])

HourlyIndexRate_PunggolYuhuaBlocks_xts <- cbind(HourlyIndexRate_PunggolBlocks_xts,HourlyIndexRate_YuhuaBlocks_xts)

# Select all / Unselect all
observeEvent(input$all_block_dataquality, {
  if (is.null(input$block_dataquality)) {
    label.block_dataquality = sort(unique(X$block))
    updateCheckboxGroupInput(
      session = session, inputId = "block_dataquality", selected = label.block_dataquality
    )
  } else {
    updateCheckboxGroupInput(
      session = session, inputId = "block_dataquality", selected = ""
    )
  }
})

output$checkboxBlock_dataquality <- renderUI({
  label.block_dataquality = sort(unique(X$block))
  checkboxGroupInput(inputId = "block_dataquality", label = "Choose", choices = label.block_dataquality, selected = sort(unique(X$block))[1:2])
})

is.selected.block_dataquality <- reactiveValues(data = NULL)
observeEvent(input$block_dataquality, {
  is.selected.block_dataquality$data <- input$block_dataquality
})


output$IndexRate_Block <- renderDygraph({
  if(!is.null(is.selected.block_dataquality$data)){
    if(is.null(input$block_dataquality)){NULL}else{
      selected <- match(input$block_dataquality,names(HourlyIndexRate_PunggolYuhuaBlocks_xts))
      graph <- dygraph(HourlyIndexRate_PunggolYuhuaBlocks_xts[,c(selected)], main = "Hourly Index Rate Per Block") %>%
        dyRangeSelector() %>%
        #dyAxis("y",label=HTML('Reading Rate (%)'),valueRange=c(round(min(HourlyIndexReadingRate_Block_xts)-5),115)) %>%
        dyAxis("y",label=HTML('Reading Rate (%)'),valueRange = c(0, 135)) %>%
        dyOptions(colors = c("#e6194b","#3cb44b","#ffe119","#0082c8","#f58231",
                             "#911eb4","#000080","#800000","#d2f53c","#fabebe","#008080","#000000"))
      graph
      }
    }else{
      graph <- dygraph(HourlyIndexRate_PunggolYuhuaBlocks_xts[,c(1,2)], main = "Hourly Index Rate Per Block") %>%  ## first two block selected
        dyRangeSelector() %>%
        #dyAxis("y",label=HTML('Reading Rate (%)'),valueRange=c(round(min(HourlyIndexReadingRate_Block_xts)-5),115)) %>%
        dyAxis("y",label=HTML('Reading Rate (%)'),valueRange = c(0, 120)) %>%
        dyOptions(colors = c("#e6194b","#3cb44b","#ffe119","#0082c8","#f58231",
                             "#911eb4","#000080","#800000","#d2f53c","#fabebe","#008080","#000000"))
    graph
  }
})

output$downloadHourlyIndexReadingRatePerBlockData <- downloadHandler(
  filename = function() { paste('HourlyIndexRatePerPunggolYuhuaBlock', '.csv', sep='') },
  content = function(file) {
    write.csv(HourlyIndexRatePerPunggolYuhuaBlock, file, row.names = FALSE)
  }
)

output$IndexRateBlock_info <- renderUI({
  fluidRow(
    column(12,HTML('<table> 
                   <tr>
                   <td> <align="justify">
                   HourlyIndexReadingRate is calculated as below:<br>
                   1) For each particular day, the HourlyIndexReadingRate is calculated for each meter. <br>
                   If for that particular meter, it has 20 counts of Index, then its HourlyIndexReadingRate
                   for that meter is 20/24=83.33.<br>
                   2) Next, grouping all those meters by date, we calculate the HourlyIndexReadingRate for each day <br>
                   as its mean of all its HourlyIndexReadingRate for each meter for that particular day. <br>
                   3) Its HourlyIndexReadingRate is then extracted for each block in Punggol.<br>
          
                   </td>
                   </tr>
                   </table>'))
  )
})

output$lastUpdated_IndexRateBlock <- renderText(Updated_DateTime_DataQuality)