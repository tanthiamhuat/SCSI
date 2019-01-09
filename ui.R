local_path <- 'D:\\DataAnalyticsPortal\\'   
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'        
path = local_path

#source("/srv/shiny-server/DataAnalyticsPortal/global.R")
source(paste0(path,'global.R'))
#source("/srv/shiny-server/DataAnalyticsPortal/source/00-dropdownButton.R",local=TRUE)
source(paste0(path,'source/00-dropdownButton.R'),local=TRUE)

#load("/srv/shiny-server/DataAnalyticsPortal/data/ZeroConsumptionCount.RData")
load(paste0(path,'data/ZeroConsumptionCount.RData'))
SuspectedMeters <- ZeroConsumptionCount %>% dplyr::filter(Comments=="Meter is suspected to be blocked.")

options(java.parameters = c("-Xss2560k", "-Xmx2g"))

thisyear <- year(today())
lastyear <- thisyear-1
# ----- Header 
header <- dashboardHeader(title = "Data Analytics Portal")

# ----- SideBar
sidebar <- dashboardSidebar(
  sidebarMenu(id="mytabs",
              sidebarMenuOutput("menu"))
)

body <- dashboardBody(
  tags$head(tags$link(rel="stylesheet", type="text/css",href="style.css"),
            tags$style(HTML("
                            @import url('//fonts.googleapis.com/css?family=Lobster|Cabin:400,700');
                            h1 {
                            background-color: #006600;
                            color: white;
                            font-family: verdana;
                            font-size: 140%;
                            # display: inline-block;
                            }
                            ")
            ),
            tags$style(HTML("
                            @import url('//fonts.googleapis.com/css?family=Lobster|Cabin:400,700');
                            h5 {
                            background-color: #FF0000;
                            color: white;
                            font-family: verdana;
                            font-size: 170%;
                            display: inline-block;
                            }
                            ")
            ),
            tags$style(HTML("
                            @import url('//fonts.googleapis.com/css?family=Lobster|Cabin:400,700');
                            h6 {
                            background-color: #00CC00;
                            color: black;
                            font-family: verdana;
                            font-size: 170%;
                            display: inline;
                            }
                            ")
            ),
            tags$style(HTML('.myClass { 
                              font-size: 20px;
                              line-height: 50px;
                              text-align: left;
                              font-family: "Helvetica Neue",Helvetica,Arial,sans-serif;
                              padding: 0 15px;
                              overflow: hidden;
                              color: white;
                              }
                           ')
            ),
            
            tags$script(type="text/javascript", src = "md5.js"),
            tags$script(type="text/javascript", src = "busy.js"),
            tags$script(type="text/javascript", src = "passwdInputBinding.js"),
            tags$script(type="text/javascript", src = "http://d3js.org/d3.v3.min.js"),
            tags$script(type="text/javascript", src="getIP.js"),
            tags$script(type="text/javascript", src = "liquidFillGauge.js"),
            tags$script(HTML('
                $(document).ready(function() {
                $("header").find("nav").append(\'<span class="myClass">Collaboration Project between Suez and Public Utilities Board (PUB) on Smart Metering Technology</span>\');
                })
           '))
           ),
  div(class = "login",
      uiOutput("uiLogin"),
      textOutput("pass")),
  div(class = "busy", 
      h4("Processing..."),
      h2(HTML('<i class="fa fa-cog fa-spin fa-2x"></i>'))
  ),
  
  tabItems(
    tabItem(tabName="dashboard",
            tags$head(tags$style(HTML(".small-box h3 {font-size: 18px}"))),
            tags$head(tags$style(HTML(".small-box .icon-large {font-size: 30px}"))),
            fluidRow(
              column(4,
                     fluidRow(
                       column(12,
                              h1('Total Consumption'),
                              plotlyOutput('TotalWeeklyConsumption_plot',width="100%",height="270px")
                       )
                     ),
                     fluidRow(
                       column(12,
                              h1('Leaks and Anomalies'),
                              plotlyOutput('TotalDailyLeak_plot',width="100%",height="200px"),
                              br(),
                              uiOutput('leaktrend_info'),
                              br(),
                              column(6,plotlyOutput('OverconsumptionAlarm_plot',width="100%",height="210px")),
                              radioButtons("site_show",
                                           label = NULL,
                                           choices = list("Punggol" = 1,"Yuhua"=2),
                                           selected = 1,inline = TRUE),
                              column(6,uiOutput('netconsumption_info'))
                       )
                     )
              ),
              column(4,
                     fluidRow(
                       column(12,
                              h1('Water Savings'),
                              column(6,uiOutput('WSAlarm_val'),uiOutput('WSAlarm'),em('With Leak Alarm'),align = 'center'),
                              column(6,uiOutput('WSGame_val'),uiOutput('WSGame'),em('With Gamification'),align = 'center')
                       )
                     ),
                     fluidRow(
                       column(12,
                              h1('Customer Engagement'),
                                  plotlyOutput('CustomerWeeklyLPCD_plot',width="100%",height="280px")
                       )
                     ),
                     fluidRow(
                       column(12,
                              h1('Overall Quantities'),
                              fluidRow(
                                column(12,
                                       DT::dataTableOutput('OverallQuantities_table')
                                )
                              )
                       )
                     )
              ),
              column(4,
                     fluidRow(
                       column(12,
                              h1('Data Quality'),
                              column(4,strong('Punggol'),div(gaugeOutput('GaugePunggol'),style = "margin-top: -20px;margin-bottom: -90px;"),em('Av. hourly reading (% over the last 30 days)'),align = 'center'),
                              column(4,strong('Yuhua'),div(gaugeOutput('GaugeYuhua'),style = "margin-top: -20px;margin-bottom: -90px;"),em('Av. hourly reading (% over the last 30 days)'),align = 'center'),
                              column(4,strong('Maintenance'),div(shinydashboard::valueBoxOutput("Maintenance",width = 16),style = "margin-left: -30px;"))
                       )
                     ),
                     fluidRow(br()),
                     fluidRow(
                       column(12,
                              plotlyOutput('signalquality_vs_distance'))
                     ),
                     fluidRow(
                       column(12,
                              h1('General Information'),
                              shinydashboard::valueBoxOutput("nbBlkBox", width = 4),
                              shinydashboard::valueBoxOutput("AMR_First", width = 4),
                              shinydashboard::valueBoxOutput("AMR_Last", width = 4)
                       )
                     ),
                     fluidRow(
                       column(12,
                              textOutput("lastUpdated_Dashboard")
                       )
                     )
              )
            )
    ),
    tabItem(tabName="SO_Cons",
            fluidRow(
              column(2,selectInput("so_period",label=h3("Period"),
                                   choices = list("Custom" = 0,
                                                  "Today" = 1,
                                                  "Yesterday" = 2,
                                                  "This week" = 3,
                                                  "Last week" = 4,
                                                  "Last 7 days" = 5,
                                                  "This month" = 6,
                                                  "Last month" = 7,
                                                  "Last 30 days" = 8,
                                                  "Last 90 days" = 9),
                                   # "This year" = 9,
                                   # "Last year" =10),
                                   selected = 5)),
              column(2,uiOutput('so_customperiod')),
              column(4,
                     checkboxGroupInput("so_show", 
                                        label = h3("Highlight"),
                                        choices = list("Weekends" = 1,"Holidays(Public, School)"=2,"Temperature" = 3),
                                        selected = NULL,inline = TRUE),
                     bsTooltip("so_show", "Weather can only be displayed with daily data",
                               "right", options = list(container = "body"))),
              column(2,radioButtons("so_timestep",label=h3("Time Step"),
                                    choices = list("Hourly" = 0,
                                                   "Daily" = 1),
                                    selected = 0, inline = TRUE)),
              column(2,radioButtons("so_site",label=h3("Site"),
                                    choices = list("Punggol" = 0,
                                                   "Yuhua" = 1),
                                    selected = 0, inline = TRUE))
            ),
            fluidRow(column(12,dygraphOutput("so_plot"))),
            fluidRow(br(),br()),
            uiOutput('so_legend_dg'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_OverallCons")))
    ),
    tabItem(tabName = "SO_MonthlyCons",
            radioButtons("MonthlyCons_site",
                         label = NULL,
                         choices = list("Punggol" = 0,"Yuhua"=1),
                         selected = 0,inline = TRUE),
            fluidRow(column(12,plotOutput('MonthlyConsumption_plot'))),
            fluidRow(br()),
            uiOutput('so_MonthlyCons_info'),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadMonthlyConsumptionData', 'Download'))),
            fluidRow(br()),
            fluidRow(column(12,plotOutput('MonthlyLPCD_plot'))),
            fluidRow(br()),
            uiOutput('so_LPCD_info'),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadMonthlyLPCDData', 'Download'))),
            fluidRow(column(12,DT::dataTableOutput('TotalHH_table'))),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_MonthlyConsumption")))
    ),
    tabItem(tabName = "SO_LPCD",
            fluidRow(column(12,dygraphOutput('WeeklyLPCD_plot'))),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadWeeklyLPCDData', 'Download'))),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_WeeklyUpdate2"))),
            fluidRow(br()),
            fluidRow(column(12,dygraphOutput('DailyLPCD_plot'))),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadDailyLPCDData', 'Download'))),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_DailyLPCD")))
    ),
    tabItem(tabName="SO_WatCons",
            fluidRow(
              column(12,selectInput("so_stepConsPattern",label=h3("What do you want to study?"),
                                    choices = list("Hourly consumptions" = 0,
                                                   "Daily consumptions" = 1),
                                    selected = 0))
            ),
            fluidRow(
              column(12,h3('Global analysis of consumption patterns'))
            ),
            fluidRow(
              column(9,plotlyOutput('so_graphGlobal_CP')),
              column(3,htmlOutput('so_textGlobal_CP'))
            ),
            fluidRow(
              column(10,h3('Detailed analysis of consumption patterns'))
            ),
            fluidRow(
              column(9,uiOutput('so_cp_seleted.dates')),column(3,uiOutput('so_stepConsPattern_tmp'))
            ),
            fluidRow(
              column(9,plotlyOutput('so_cp_seleted.plot')),
              column(3,htmlOutput('so_cp_seleted.message'))
            )
    ),
    tabItem(tabName = "SO_National",
            fluidRow(
              column(4,
                     selectInput("RoomType",
                                 "RoomType",
                                 c("All","HDB01","HDB02","HDB03","HDB04","HDB05"))),
              column(4,
                     selectInput("Year",
                                 "Year",
                                 c(lastyear,thisyear)))
              ),
            fluidRow(
              DT::dataTableOutput("NationalAverageRoomType_table")
            ),
            fluidRow(br()),
            uiOutput('NationalAverageRoomType_info'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_NationalAverageRoomType"))),
            fluidRow(br()),
            fluidRow(column(12,plotOutput('NationalAverageRoomType_plot')))
    ),
    tabItem(tabName = "BL_Consumption",
            fluidRow(
              column(
                width = 6,
                dropdownButton(
                  label = "Block", status = "default", width = 80,
                  actionButton(inputId = "all_block_consumption", label = "(Un)select all"),
                  uiOutput('checkboxBlock_consumption')
                )
              ),
              column(6,radioButtons("blockconsumption_timestep",
                                    label = h3("Time Step"),
                                    choices = list("Hourly" = 0, "Daily" = 1),
                                    selected = 0,
                                    inline = TRUE
              ))
            ),
            fluidRow(column(12,dygraphOutput("bl_plot_global"))
            ),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadBlockConsumptionData', 'Download'))),
            fluidRow(br()),
            uiOutput('block_info'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_ConsumptionPerBlock")))
    ),
    tabItem(tabName="BL_LPCD_NationalAverage",
            fluidRow(column(12,plotOutput('BlockLPCD_NationalAverage_plot1'))),
            fluidRow(br()),
            fluidRow(column(12,plotOutput('RoomTypeLPCD_NationalAverage_plot2'))),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_BlockNationalAverageLPCD")))
    ),
    tabItem(tabName="BL_WatSav",
            fluidRow(column(12,plotlyOutput('BlockSavings_plot'))),
            fluidRow(br(),br()),
            fluidRow(column(12,DT::dataTableOutput('BlockSavings_table'))),
            fluidRow(br()),
            uiOutput('BlockSavings_info'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_BlockSavings")))
    ),
    tabItem(tabName="BL_Forecast",
            fluidRow(column(12,plotOutput('Block_Forecast_plot'))),
            fluidRow(br()),
            uiOutput('block_forecast_info'),
            fluidRow(br()),
            fluidRow(column(12,dygraphOutput('Overall_Forecast_plot'))),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_BlockForecast")))
    ),
    tabItem(tabName="BL_NetConsumption",
            fluidRow(
              column(
                width = 6,
                dropdownButton(
                  label = "Block", status = "default", width = 80,
                  actionButton(inputId = "all_block_netconsumption", label = "(Un)select all"),
                  uiOutput('checkboxBlock_net')
                )
              ),
              column(6,radioButtons("NetConsumption_timestep",
                                    label = h3("Time Step"),
                                    choices = list("Weekly" = 0, "Monthly" = 1),
                                    selected = 0,inline = TRUE
              ))
            ),
            fluidRow(column(12,dygraphOutput('block_NetConsumption_plot1'))),
            fluidRow(br(),br()),
            fluidRow(column(12,dygraphOutput('block_NetConsumption_plot2'))),
            fluidRow(br(),br()),
            fluidRow(
              column(8,DT::dataTableOutput('block_NetConsumption_table')),
              br(),br(),br(),br(),
              column(4,uiOutput('block_NetConsumption_info'))
            ),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadNetConsumptionData', 'Download'))),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_NetConsumption")))
    ),
    tabItem(tabName="BL_NetConsumptionDetails",
               fluidRow(
                 column(3,
                        selectInput("NetConsumptionDetails_Block",
                                    "Block",
                                    c("PG_B1","PG_B2","PG_B3","PG_B4","PG_B5")
                                    )
                 ),
                 column(3,radioButtons("NetConsumptionDetails_Supply", label = "Supply",
                          choices = list("Direct" = 0, "Indirect" = 1),
                          selected = 0,inline = TRUE)
                 )
               ),
               fluidRow(column(12,DT::dataTableOutput('block_NetConsumptionDetails_table'))),
               fluidRow(br()),
               fluidRow(column(12,downloadButton('downloadWeeklyPunggolConsumptionData','Download'))),
               fluidRow(br()),
               fluidRow(column(12,textOutput("lastUpdated_NetConsumptionDetails_Weekly")))
    ),
    tabItem(tabName="CP_Segmentation",
            fluidRow(column(12,dygraphOutput('CustomerSegmentation_plot'))),
            fluidRow(br()),
            uiOutput('CustomerSegmentation_info'),
            fluidRow(br()),
            fluidRow(column(12,DT::dataTableOutput('Customers_table'))),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_CustomerSegmentation")))
    ),
    tabItem(tabName="CP_Benchmark",
            fluidRow(column(12,DT::dataTableOutput('CustomerProfileBenchmark_table'))),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_CustomerProfileBenchmark")))
    ),
    tabItem(tabName="CP_WaterSavings",
            fluidRow(column(12,dygraphOutput('CustomerProfile_WaterSavings_plot'))),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_CustomerProfileWaterSavings")))
    ),
    tabItem(tabName="CA_Summary",
            radioButtons("CA_Summary_site",
                         label = NULL,
                         choices = list("Punggol" = 0,"Yuhua"=1),
                         selected = 0,inline = TRUE),
            fluidRow(column(6,align="left",
                     splitLayout(cellWidths=c("50%", "50%"),plotlyOutput('CustomerAgeRange_plot'),
                                                            plotlyOutput('CustomerLoginFreq_plot')
                                )
                     ),
                     column(6,plotlyOutput('VisitorsPerDay_plot'))
            ),
            fluidRow(br()),
            fluidRow(column(3,downloadButton('downloadCustomerAgeRangeData','Download')),
                     column(3,downloadButton('downloadCustomerLoginFrequencyData','Download')),
                     column(6,downloadButton('downloadVisitorsPerDayData','Download'))
            ),
            fluidRow(br()),
            fluidRow(column(6,plotlyOutput('BlockOnlineOffline_plot')),
                     column(6,plotlyOutput('CustomerSignUp_plot'))
            ),
            fluidRow(br()),
            fluidRow(column(6,downloadButton('downloadBlockOnlineOfflineData','Download')),
                     column(6,downloadButton('downloadCustomerSignUpData','Download'))
            ),
            fluidRow(br()),
            fluidRow(column(6,plotlyOutput('CustomerAccumulativePoints_plot')),
                     column(6,plotlyOutput('CustomerAccumulatedPoints_Histogram'))
            ),
            fluidRow(br()),
            fluidRow(column(6,downloadButton('downloadCustomerAccumulativePointsData','Download')),
                     column(6,downloadButton('downloadCustomerAccumulatedPointsData','Download'))
            ),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_CustomerSummary")))
    ),
    tabItem(tabName="CA_WeeklyLPCD",
            fluidRow(column(12,plotlyOutput('CAWeeklyLPCD_plot'))),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadWeeklyLPCDOnlineOfflineData', 'Download'))),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_WeeklyLPCD")))
    ),
    tabItem(tabName="CA_OnlineLPCDPerFamilySize",
            fluidRow(column(12,plotlyOutput('CAOnlineLPCDPerFamilySize_plot'))),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadOnlineLPCDPerFamilySizeData', 'Download'))),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_OnlineLPCDPerFamilySize")))
    ),
    tabItem(tabName="AL_LeakSeverity",
            fluidRow(column(12,D3TableFilter::d3tfOutput('LeakAlarm_D3table',height="auto"))),
            fluidRow(column(2,h4("SUEZ INTELLECTUAL PROPERTY")),
                     column(1,actionButton("save_leak", "Save")),
                     column(1,downloadButton('downloadLeakSeverityData', 'Download'))
                    ),
            fluidRow(br()),
            fluidRow(column(6,plotlyOutput('LeakInfo_plot',width = "100%", height = "400px")),
                     column(6,uiOutput('LeakSeverity_info'))
                     ),
            fluidRow(br()),
            fluidRow(column(12,plotlyOutput('QtyLeaksOpenClose_plot',width = "100%", height = "800px"))),
            fluidRow(br()),
            uiOutput('QtyLeaksOpenClose_info'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_LeakSeverity")))
    ),
    tabItem(tabName="AL_LeakVolume",
            fluidRow(column(12,dygraphOutput('LeakVolume_plot1'))),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadLeakVolumeData', 'Download'))),
            fluidRow(br()),
            fluidRow(
              column(
                width = 6,
                dropdownButton(
                  label = "Block", status = "default", width = 80,
                  actionButton(inputId = "all_leakvolume", label = "(Un)select all"),
                  uiOutput('checkboxBlock_leakvolume')
                )
              )
            ),
            fluidRow(column(12,dygraphOutput('LeakVolume_plot2'))),
            fluidRow(br()),
            fluidRow(column(12,h4("SUEZ INTELLECTUAL PROPERTY"))),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadLeakVolumePerBlockData', 'Download'))),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_LeakVolume"))),
            fluidRow(br()),
            uiOutput('LeakVolume_info')
    ),
    tabItem(tabName="AL_OverConsumption",
            fluidRow(column(12,DT::dataTableOutput('OverConsumption_table'))),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadOverConsumptionData', 'Download'))),
            fluidRow(br()),
            uiOutput('OverConsumption_info'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_OverConsumption")))
    ),
    tabItem(tabName="AL_ZeroConsumption",
            fluidRow(column(12,DT::dataTableOutput('ZeroConsumption_table'))),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadZeroConsumptionData', 'Download'))),
            fluidRow(br()),
            uiOutput('ZeroConsumption_info'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_ZeroConsumption")))
    ),
    tabItem(tabName="AL_MetersStop",
            fluidRow(column(4,selectInput("MetersSuspicion",
                                          "Meters Under Suspicion",
                                          c(SuspectedMeters$meter_sn))),
                                          #c("WS500376A","WS500368A","WS500364A"))),
                     column(4,textOutput("MeterStop_block"),textOutput("MeterStop_supply"))
            ),
            fluidRow(column(12,plotlyOutput('MeterStop_plot')))
    ),
    tabItem(tabName="AL_UnexpectedConsumption",
            fluidRow(column(12,DT::dataTableOutput('UnexpectedConsumption_table'))),
            uiOutput('UnexpectedConsumption_info'),
            fluidRow(column(12,h4("SUEZ INTELLECTUAL PROPERTY"))),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_UnexpectedConsumption")))
    ),
    tabItem(tabName="ED_Statistics",
            fluidRow(column(12,h5("Be extra careful in this section. A change in the values will affect the tables in the Database."))),
            fluidRow(column(12,uiOutput("WaterPrice_slider"),
                            uiOutput("WaterPrice_Save"))),
            fluidRow(column(12,uiOutput("LPCD_slider"),
                            uiOutput("LPCD_Save")))
    ),
    tabItem(tabName="DQ_OverallPerformance",
            fluidRow(column(12,dygraphOutput('Overall_Performance'))),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadOverallPerformanceData', 'Download'))),
            fluidRow(br()),
            uiOutput('OverallPerformance_info'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_OverallPerformance")))
            
    ),
    tabItem(tabName="DQ_IndexRateArea",
            fluidRow(column(12,dygraphOutput('IndexRate_Area'))),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadHourlyIndexReadingRatePerAreaData', 'Download'))),
            fluidRow(br()),
            uiOutput('IndexRateArea_info'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_IndexRateArea")))
    ),
    tabItem(tabName="DQ_IndexRateBlock",
            fluidRow(
              column(
                width = 6,
                dropdownButton(
                  label = "Block", status = "default", width = 80,
                  actionButton(inputId = "all_block_dataquality", label = "(Un)select all"),
                  uiOutput('checkboxBlock_dataquality')
                )
              )),
            fluidRow(column(12,dygraphOutput('IndexRate_Block'))),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadHourlyIndexReadingRatePerBlockData', 'Download'))),
            fluidRow(br()),
            uiOutput('IndexRateBlock_info'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_IndexRateBlock")))
    ),
    tabItem(tabName="DQ_BillableMetersIndexChecks",
            fluidRow(column(12,plotlyOutput('BillableMeters_IndexChecks'))),
            fluidRow(br()),
            fluidRow(column(12,h4("SUEZ INTELLECTUAL PROPERTY"))),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadBillableMetersData', 'Download'))),
            fluidRow(br()),
            fluidRow(column(12,textOutput('ZeroIndex_ServicePointSn_text'))),
            fluidRow(br()),
            fluidRow(column(12,DT::dataTableOutput('ZeroIndex_ServicePointSn_table'))),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadZeroIndexServicePointData', 'Download'))),
            fluidRow(br()),
            fluidRow(column(12,textOutput('IncompleteIndex_ServicePointSn_text'))),
            fluidRow(br()),
            fluidRow(column(12,DT::dataTableOutput('IncompleteIndex_ServicePointSn_table'))),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_BillableMeters")))
    ),
    tabItem(tabName="DQ_MapDisplay",
            fluidRow(
              column(10,
                radioButtons("past_days", 
                           label = NULL,
                           choices = list("Last 5 days" = 0,"Last 30 days"=1),
                           selected = 0,inline = TRUE)
              ),
              column(12,leafletOutput('sgp_aws'))
            ),
            fluidRow(
              column(6,uiOutput('Map_info')),
              column(6,uiOutput("ReadingRateImage"))
            ),
            fluidRow(
              column(2,DT::dataTableOutput('MapDisplay_table')),
              column(10,fluidRow(br()),
                        fluidRow(br()),
                        fluidRow(br()),
                        fluidRow(br()),
                     plotlyOutput('MapDisplay_plot'),
              fluidRow(column(12,textOutput("lastUpdated_MapDisplay")))
              )
            )
    ),
    tabItem(tabName="DQ_HourlyConsumptionRate",
            fluidRow(column(12,plotlyOutput('HourlyConsumptionRate'))),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadConsumptionRateData', 'Download'))),
            fluidRow(br()),
            uiOutput('ConsumptionRate_info'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_ConsumptionRate")))
    ),
    tabItem(tabName="DQ_Counts24PerDay",
            fluidRow(column(12,DT::dataTableOutput('Counts24PerDay_table'))),
            fluidRow(br()),
            fluidRow(column(12,downloadButton('downloadCounts24PerDayData', 'Download'))),
            fluidRow(br()),
            uiOutput('Counts24PerDay_info'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_Counts24PerDay")))
    ),
    tabItem(tabName="DQ_DataFreshness",
            fluidRow(column(12,plotlyOutput('DataFreshness_plot'))),
            fluidRow(br()),
            uiOutput('DataFreshness_info'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_DataFreshness")))
    ),
    tabItem(tabName="DD_ConsumptionData",
            fluidRow(
              column(3,selectInput("Consumptiondatadownload_period",label=h3("Period"),
                                   choices = list("Custom" = 0,
                                                  "Today" = 1,
                                                  "Yesterday" = 2,
                                                  "This week" = 3,
                                                  "Last week" = 4,
                                                  "Last 7 days" = 5,
                                                  "This month" = 6,
                                                  "Last month" = 7,
                                                  "Last 30 days" = 8),
                                   selected = 5)),
              column(3,uiOutput('Consumptiondatadownload_customperiod'))
            ),
            fluidRow(br()),
            fluidRow(column(12,DT::dataTableOutput('Consumption_table'))),
            fluidRow(br()), 
            fluidRow(column(12,downloadButton('downloadConsumptionData', 'Download'))),
            fluidRow(br()),
            uiOutput('Consumption_table_info'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_downloadConsumption")))
    ),
    tabItem(tabName="DD_RawIndexData",
            fluidRow(
              column(3,selectInput("Indexdatadownload_period",label=h3("Period"),
                                   choices = list("Custom" = 0,
                                                  "Today" = 1,
                                                  "Yesterday" = 2,
                                                  "This week" = 3,
                                                  "Last week" = 4,
                                                  "Last 7 days" = 5,
                                                  "This month" = 6,
                                                  "Last month" = 7,
                                                  "Last 30 days" = 8),
                                   selected = 5)),
              column(3,uiOutput('Indexdatadownload_customperiod'))
            ),
            fluidRow(br()),
            fluidRow(column(12,DT::dataTableOutput('Index_table'))),
            fluidRow(br()), 
            fluidRow(column(12,downloadButton('downloadIndexData', 'Download'))),
            fluidRow(br()),
            uiOutput('Index_table_info'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_downloadIndex")))
    ),
    tabItem(tabName="DI_OndeoAWS",
            fluidRow(column(6,DT::dataTableOutput('OndeoAWS_ServicePointSn_table')),
                     column(6,radioButtons("ServicePointSn_Inconsistency",label=h3("Inconsistency Due to:"),
                                           choices = list("Consumption" = 0,
                                                          "Index" = 1,
                                                          "MeterSn" = 2,
                                                          "PubCustId" = 3,
                                                          "ServicePointSn" = 4),
                                           selected = 0, inline = TRUE),
                            uiOutput('DataInconsistency_info'))),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_OndeoAWS")))
    ),
    tabItem(tabName="DI_InsertTimeLag",
            fluidRow(column(12,dygraphOutput('InsertTimeLag_PerServer'))),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_InsertTimeLag")))
    ),
    tabItem(tabName="DI_AWSTables",
            fluidRow(column(12,DT::dataTableOutput('AWSTable1_table'))),
            uiOutput('AWSTable1_info'),
            fluidRow(br()),
            fluidRow(column(12,DT::dataTableOutput('AWSTable2_table'))),
            uiOutput('AWSTable2_info'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_AWSTables")))
    ),
    tabItem(tabName="DI_CountsDiscrepancies",
            fluidRow(column(12,DT::dataTableOutput('ConsumptionCountsDiscrepancies_table'))),
            fluidRow(br()),
            fluidRow(column(12,DT::dataTableOutput('IndexCountsDiscrepancies_table'))),
            uiOutput('CountsDiscrepancies_info'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_CountsDiscrepancies")))
    ),
    tabItem(tabName="DI_NAConsumption",
            fluidRow(column(12,plotlyOutput('MeterCountFull_24ContainsNA_Plot'))),
            fluidRow(br()),
            uiOutput('NAConsumption_info'),
            fluidRow(br()),
            fluidRow(column(12,textOutput("lastUpdated_NAConsumption")))
    ),
    tabItem(tabName="Logout",
            fluidRow(br(),br()),
            uiOutput('Logout')
    )
  ))

dashboardPage(header, sidebar, body,skin="green")