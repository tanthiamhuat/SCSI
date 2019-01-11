local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

source(paste0(path,'AutoUpdated_RScripts/ToDisconnect.R'))
source(paste0(path,'AutoUpdated_RScripts/DB_Connections.R'))

leak_alarm <- as.data.frame(tbl(con,"leak_alarm"))

output$WaterPrice_slider <- renderUI({
  sliderInput("WaterPrice", h3(HTML('Price Per Cubic meters (S$/m<sup>3</sup>)')),
               min=2,max=10,value=2.56,step= 0.01, width='30%')
})

output$WaterPrice_Save <- renderUI({
  tagList(
    bsModal("modalSave_WaterPrice", "Save Water Price", "save_tmp_WaterPrice", size = "small",wellPanel(
      h3(paste0('Do you confirm a water price of $', input$WaterPrice,'?\n')),
      actionButton("save_WaterPrice", "Save"),
      actionButton("cancel_WaterPrice", "Cancel")
    )),
    actionButton('save_tmp_WaterPrice','Save')
  )
})

observeEvent(input$save_WaterPrice,{
  UpdatedWaterPrice <- input$WaterPrice
  leak_alarm <- leak_alarm %>% dplyr::mutate(potential_savings=round(min5flow_ave*24*365*UpdatedWaterPrice/1000))
  sql_update <- paste("UPDATE leak_alarm SET potential_savings = '",leak_alarm$potential_savings,"'
                       WHERE start_date = '",leak_alarm$start_date,"' and
                       service_point_sn = '",leak_alarm$service_point_sn,"' and
                      meter_sn = '",leak_alarm$meter_sn,"' ",sep="")
  sapply(sql_update, function(x){dbSendQuery(mydb, x)})
  toggleModal(session,'modalSave_WaterPrice','close')
})
  
observeEvent(input$cancel_WaterPrice,{
  toggleModal(session,'modalSave_WaterPrice','close')
})

output$LPCD_slider <- renderUI({
  sliderInput("LPCD", h3(HTML('Litres per Capita per Day (LPCD)')),
              min=120,max=150,value=130,step= 1, width='30%')
})

output$LPCD_Save <- renderUI({
  tagList(
    bsModal("modalSave_LPCD", "Save LPCD", "save_tmp_LPCD", size = "small",wellPanel(
      h3(paste0('Do you confirm a LPCD of ', input$LPCD,'?\n')),
      actionButton("save_LPCD", "Save"),
      actionButton("cancel_LPCD", "Cancel")
    )),
    actionButton('save_tmp_LPCD','Save')
  )
})

observeEvent(input$save_LPCD,{
  UpdatedLPCD <- input$LPCD
  values.LPCD$LPCD <- UpdatedLPCD
  save(UpdatedLPCD,file=paste0(path,'data/UpdatedLPCD.RData')) 
  toggleModal(session,'modalSave_LPCD','close')
})

observeEvent(input$cancel_LPCD,{
  toggleModal(session,'modalSave_LPCD','close')
})
