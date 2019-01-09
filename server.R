local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'    

shinyServer(function(input,output,session) {
  cat('Loading shiny\n')
  
   output$menu <- renderMenu({
    sidebarMenu(
      menuItem("Login page",tabName = "login", icon = icon("key"))
    )
  })
  
  source(paste0(path,"www/Login.R"),local = TRUE)
  
  observe({
    if (USER$Logged == TRUE) {
      cat('Logged\n')
      Sys.setenv(TZ='Asia/Singapore')
      
      load(paste0(path,"data/Week.date_Updated_DateTime_HourlyCons.RData"))
     
      ## from /data/DT directory for Punggol
      Punggol_All <- read.fst(paste0(path,"data/DT/Punggol_last90days.fst"),as.data.table=T)
      
      Main_BPass <- Punggol_All[room_type == "NIL"] # contain data on main meter and bypass
      Punggol_SUB <- Punggol_All[!room_type %in% c("NIL","HDBCD") & adjusted_consumption != "NA"] # contain data on sub meter, excluding childcare
    
      ## from /data/DT directory for Yuhua
      Yuhua_All <- read.fst(paste0(path,"data/DT/Yuhua_last90days.fst"),as.data.table=T)
      
      Main_BPass <- Yuhua_All[meter_type %in% c("MAIN","BYPASS")] # contain data on main meter and bypass
      Yuhua_SUB <- Yuhua_All[meter_type == "SUB" & !room_type %in% c("Nil","OTHER") & adjusted_consumption != "NA"] # contain data on sub meter, excluding shophouse
      
      X <- rbind(Punggol_SUB,Yuhua_SUB)
      X <- X %>% filter(service_point_sn !="3100660792")
      
      load(paste0(path,'data/Weather.RData'))

      cat('Data loaded \n')
       if(isolate(input$userName) == 'administrator'){
         output$menu <- renderMenu({
           sidebarMenu(id = 'sbDAP',
             menuItem("Dashboard", tabName = "dashboard", icon = icon("home"),selected = TRUE),
             menuItem("Summary Overview", tabName = "SummryOverview", icon = icon("line-chart"),
                      menuSubItem("Overall Consumption", tabName = "SO_Cons"),
                      menuSubItem("Monthly Consumption", tabName = "SO_MonthlyCons"),
                      menuSubItem("LPCD & Occupancy", tabName = "SO_LPCD"),
                      menuSubItem("Water Consumption Patterns", tabName = "SO_WatCons"),
                      menuSubItem("National Average (RoomType)", tabName = "SO_National")),
             menuItem("Block", tabName = "conso", icon = icon("building-o"),
                      menuSubItem("Consumption Per Block",tabName = "BL_Consumption"),
                      menuSubItem("National Average (LPCD)", tabName = "BL_LPCD_NationalAverage"),
                      menuSubItem("Water Savings", tabName = "BL_WatSav"),
                      menuSubItem("Forecast", tabName = "BL_Forecast"),
                      menuSubItem("Net Consumption", tabName = "BL_NetConsumption"),
                      menuSubItem("Net Consumption Details", tabName = "BL_NetConsumptionDetails")),
             menuItem("Customer Profile", tabName = "conso", icon = icon("users"),
                      menuSubItem("Customer Segmentation", tabName = "CP_Segmentation"),
                      menuSubItem("Benchmark", tabName = "CP_Benchmark")),
                 #     menuSubItem("Water Savings", tabName = "CP_WaterSavings")),
             menuItem("Customer Analysis", tabName = "analysis", icon = icon("users"),
                      menuSubItem("Customer Summary", tabName = "CA_Summary"),
                      menuSubItem("Weekly LPCD", tabName = "CA_WeeklyLPCD"),
                      menuSubItem("Online LPCD Per Family Size", tabName = "CA_OnlineLPCDPerFamilySize")),
             menuItem("Leak Alert and Anomalies", tabName = "AL_Anomalies", icon = icon("bell"),
                      menuSubItem("Leaks", tabName = "AL_LeakSeverity"),
                      menuSubItem("Leak Volume", tabName = "AL_LeakVolume"),
                      menuSubItem("Overconsumption", tabName = "AL_OverConsumption"),
                      menuSubItem("Zero Consumption", tabName = "AL_ZeroConsumption"),
                      menuSubItem("Meters Stop Suspicion", tabName = "AL_MetersStop"),
                      menuSubItem("Unexpected Consumption", tabName = "AL_UnexpectedConsumption")),
             menuItem("Editable Values", tabName = "ED_Values", icon = icon("calculator"),
                      menuSubItem("Statistics and Water Price", tabName = "ED_Statistics")),
             menuItem("Data Quality", tabName = "DataQuality", icon = icon("database"),
                      menuSubItem("Overall Performance", tabName = "DQ_OverallPerformance"),
                      menuSubItem("Hourly Index Rate Per Area", tabName = "DQ_IndexRateArea"),
                      menuSubItem("Hourly Index Rate Per Block", tabName = "DQ_IndexRateBlock"),
                      menuSubItem("Billable Meters Index Checks", tabName = "DQ_BillableMetersIndexChecks"),
                      menuSubItem("Map Display", tabName = "DQ_MapDisplay"),
                      menuSubItem("Hourly Consumption Rate", tabName = "DQ_HourlyConsumptionRate"),
                      menuSubItem("Counts 24 PerDay", tabName = "DQ_Counts24PerDay"),
                      menuSubItem("Data Freshness", tabName = "DQ_DataFreshness")),
             menuItem("Data Download", tabName = "DataDownload", icon = icon("download"),
                      menuSubItem("Consumption", tabName = "DD_ConsumptionData"),
                      menuSubItem("Raw Index", tabName = "DD_RawIndexData")),
             # menuItem("Data Inconsistency", tabName = "DataInconsistency", icon = icon("database"),
             #          menuSubItem("Ondeo vs AWS", tabName = "DI_OndeoAWS"),
             #          menuSubItem("Insert Time Lag", tabName = "DI_InsertTimeLag"),
             #        #  menuSubItem("AWS Tables", tabName = "DI_AWSTables"),
             #        #  menuSubItem("Counts Discrepancies", tabName = "DI_CountsDiscrepancies"),
             #          menuSubItem("NA Consumption", tabName = "DI_NAConsumption")),
             menuItem("Logout", tabName = "Logout", icon = icon("sign-out")) 
           )
         })
       }else{
         output$menu <- renderMenu({
           sidebarMenu(id = 'sbDAP',
             menuItem("Dashboard", tabName = "dashboard", icon = icon("home"),selected = TRUE),
             #menuItem("Home Page", tabName = "homepage", icon = icon("home"),selected = TRUE),
             menuItem("Summary Overview", tabName = "SummryOverview", icon = icon("line-chart"),
                      menuSubItem("Overall Consumption", tabName = "SO_Cons"),
                      menuSubItem("Monthly Consumption", tabName = "SO_MonthlyCons"),
                      menuSubItem("LPCD & Occupancy", tabName = "SO_LPCD"),
                      menuSubItem("Water Consumption Patterns", tabName = "SO_WatCons"),
                      menuSubItem("National Average", tabName = "SO_National")),
             menuItem("Block", tabName = "conso", icon = icon("building-o"),
                      menuSubItem("Consumption Per Block",tabName = "BL_Consumption"),
                      menuSubItem("National Average", tabName = "BL_LPCD_NationalAverage"),
                      menuSubItem("Water Savings", tabName = "BL_WatSav"),
                      menuSubItem("Forecast", tabName = "BL_Forecast"),
                      menuSubItem("Net Consumption", tabName = "BL_NetConsumption")),
             menuItem("Customer Profile", tabName = "conso", icon = icon("users"),
                      menuSubItem("Customer Segmentation", tabName = "CP_Segmentation1"),
                      menuSubItem("Benchmark", tabName = "CP_Benchmark"),
                      menuSubItem("Water Savings", tabName = "CP_WatSav")),
             menuItem("Leak Alert and Anomalies", tabName = "AL_Anomalies", icon = icon("bell"),
                      menuSubItem("Leaks", tabName = "AL_LeakSeverity"),
                      menuSubItem("Leak Volume", tabName = "AL_LeakVolume"),
                      menuSubItem("Overconsumption", tabName = "AL_OverConsumption"),
                      menuSubItem("Zero Consumption", tabName = "AL_ZeroConsumption"),
                      menuSubItem("Unexpected Consumption", tabName = "AL_UnexpectedConsumption")),
             menuItem("Data Quality", tabName = "DataQuality", icon = icon("database"),
                      menuSubItem("Overall Performance", tabName = "DQ_OverallPerformance"),
                      menuSubItem("Hourly Index Rate Per Area", tabName = "DQ_IndexRateArea"),
                      menuSubItem("Hourly Index Rate Per Block", tabName = "DQ_IndexRateBlock"),
                      menuSubItem("Billable Meters Index Checks", tabName = "DQ_BillableMetersIndexChecks")),
             menuItem("Data Download", tabName = "DataDownload", icon = icon("download"),
                      menuSubItem("Consumption", tabName = "DD_ConsumptionData"),
                      menuSubItem("Raw Index", tabName = "DD_RawIndexData")),
             menuItem("Logout", tabName = "Logout", icon = icon("sign-out")) 
           )
         })
       }
      values.loadedPages <- reactiveValues(pages=c()) 
      values.LPCD <- reactiveValues(LPCD = NULL)
      load(paste0(path,"data/UpdatedLPCD.RData"))
      values.LPCD$LPCD <- UpdatedLPCD
      
      observeEvent(input$mytabs,{
        page <- substr(input$mytabs,1,2)
        if(!page %in% values.loadedPages$pages){
          if(page == 'SO'){  
            source(paste0(path,'source/02a-OverallConsumption.R'),local = TRUE)
            source(paste0(path,'source/02b-MonthlyConsumption.R'),local = TRUE)
            source(paste0(path,'source/02c-LPCDValues.R'),local = TRUE)
            source(paste0(path,'source/02d-WaterConsumptionPatterns.R'),local = TRUE)
            source(paste0(path,'source/02e-NationalAverageRoomType.R'),local = TRUE)
          }
          if(page == 'BL'){  
            source(paste0(path,'source/03a-ConsumptionPerBlock.R'),local = TRUE)
            source(paste0(path,'source/03b-Block_NationalAverageLPCD.R'),local = TRUE)
            source(paste0(path,'source/03c-Block_WaterSavings.R'),local = TRUE)
            source(paste0(path,'source/03d-Block_Forecast.R'),local = TRUE)
            source(paste0(path,'source/03e-Block_NetConsumption.R'),local = TRUE)
            source(paste0(path,'source/03f-Block_NetConsumptionDetails.R'),local = TRUE)
          }
          if(page == 'CP'){
            source(paste0(path,'source/04a-CustomerSegmentation.R'),local = TRUE)
            source(paste0(path,'source/04b-CustomerProfile_Benchmark.R'),local = TRUE)
          #  source(paste0(path,'source/04c-CustomerProfile_WaterSavings.R'),local = TRUE)
          }
          if(page == 'CA'){
            source(paste0(path,'source/05a-CustomerSummary.R'),local = TRUE)
            source(paste0(path,'source/05b-WeeklyLPCD.R'),local = TRUE)
            source(paste0(path,'source/05c-OnlineLPCDPerFamilySize.R'),local = TRUE)
          }
          if(page == 'AL'){
            source(paste0(path,'source/06a-Leaks.R'),local = TRUE)
            source(paste0(path,'source/06b-LeakVolume.R'),local = TRUE)
            source(paste0(path,'source/06c-OverConsumption.R'),local = TRUE)
            source(paste0(path,'source/06d-ZeroConsumption.R'),local = TRUE)
            source(paste0(path,'source/06e-MetersStopSuspicion.R'),local = TRUE)
            source(paste0(path,'source/06f-UnexpectedConsumption.R'),local = TRUE)
          }
          if(page == 'ED'){
            source(paste0(path,'source/07a-Statistics_WaterPrice.R'),local = TRUE)
          }
          if(page == 'DQ'){
            source(paste0(path,'source/08a-DataQuality_OverallPerformance.R'),local = TRUE)   
            source(paste0(path,'source/08b-DataQuality_HourlyIndexRatePerArea.R'),local = TRUE)
            source(paste0(path,'source/08c-DataQuality_HourlyIndexRatePerBlock.R'),local = TRUE)
            source(paste0(path,'source/08d-DataQuality_BillableMetersIndexChecks.R'),local = TRUE)
            source(paste0(path,'source/08e-DataQuality_MapDisplay.R'),local = TRUE)
            source(paste0(path,'source/08f-DataQuality_HourlyConsumptionRate.R'),local = TRUE)
            source(paste0(path,'source/08g-DataQuality_Counts24PerDay.R'),local = TRUE) 
            source(paste0(path,'source/08h-DataQuality_DataFreshness.R'),local = TRUE) 
          }
          if(page == 'DD'){
            source(paste0(path,'source/09a-DataDownload_Consumption.R'),local = TRUE)
            source(paste0(path,'source/09b-DataDownload_RawIndex.R'),local = TRUE)
          }
          # if(page == 'DI'){
          #   source(paste0(path,'source/11a-DataInconsistency_OndeoAWS.R'),local = TRUE)
          #   source(paste0(path,'source/11b-DataInconsistency_InsertTimeLag.R'),local = TRUE)
          #   source(paste0(path,'source/11c-DataInconsistency_AWSTables.R'),local = TRUE)
          #   source(paste0(path,'source/11d-DataInconsistency_CountsDiscrepancies.R'),local = TRUE)
          #   source(paste0(path,'source/11e-DataInconsistency_NAConsumption.R'),local = TRUE)
          # }
          values.loadedPages$pages <- c(values.loadedPages$pages,page)
        }
      })
      
  ####  Home Page #####
  source(paste0(path,'source/01-Dashboard.R'), local=TRUE)
  #####  Logout #####
  source(paste0(path,'source/10-Logout.R'),local = TRUE)  
}
    updateTabItems(session, "mytabs", "dashboard")
  
  })
})
