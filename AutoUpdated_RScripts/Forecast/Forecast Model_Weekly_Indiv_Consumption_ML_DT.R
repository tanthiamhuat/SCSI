###########################
# LOADING PACKAGES        #
###########################
if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(DBI,RPostgreSQL,foreach,randomForest,e1071,plyr,dplyr,magrittr,ggplot2,lubridate,doSNOW,parallel,snow,boot,stats,xts,forecast,nnet,fst,stringr,ISOweek)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')
load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")

holidays <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/Holidays_2014_2017.csv",sep = ";",header = FALSE)
colnames(holidays) <- c("Holidays","Date")
holidays$Date <- dmy(holidays$Date)

family <- as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))

servicepoint <- as.data.frame(tbl(con,"service_point")) %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B")

family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id","room_type"))

set.seed(123)

##########################
# LOADING DATA           #
##########################
PunggolYuhua <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/DT/consumption_last12months_servicepoint.fst",as.data.table = TRUE) %>%
                filter(site %in% c("Punggol","Yuhua") & room_type %in% c("HDB01","HDB02","HDB03","HDB04","HDB05") & 
                       adjusted_consumption != 'NA' & meter_type =="SUB") %>% 
                dplyr::mutate(week = gsub("-W","_",str_sub(date2ISOweek(date(date_consumption)),end = -3)),Date=date(date_consumption)) # convert date to week
X <- PunggolYuhua
X$holiday <- (X$Date %in% holidays$Date)*1

X1 <- inner_join(X,family_servicepoint,by=c("service_point_sn","block","floor","unit","site","meter_type","room_type")) %>%
      dplyr::filter(date(date_consumption) >= move_in_date)  # take into consideration of move-in-date
                                                           
Indiv <- X1 %>% group_by(service_point_sn,week) %>% dplyr::summarise(Consumption=sum(adjusted_consumption),
                                                                     Holidays=sum(holiday)) 

# weekly consumption
Indiv$Date <- Week.date$beg[match(Indiv$week,Week.date$week)]

load("/srv/shiny-server/DataAnalyticsPortal/data/Select Forecast Model_Weekly_Indiv_Consumption.RData")

# Indiv <- Indiv %>% filter(week != '2016_31')
# my_data <- X %>% filter(service_point_sn == X$service_point_sn[1])  # daily
# my_data <- X %>% filter(service_point_sn == sn)

#' Forecast is a function currently providing prediction of weekly consumption at W+1
#' it has the following argument
#' @my_data a dataframe of weekly consumption for a specific block (a subset of Block)
#' @model a character specificying the forecast model to use, must be one of ("ML_RF","ML_SVM","TS_HWexpS","TS_HW","TS_ARIMA")
#' for different models for each customer, provide value for @specify.model
#' @specify.model a dataframe specifying for each customer the model to use
#' it should contain at least the following column 'service_point_sn' and 'bestmodel'

# my_data <- Indiv %>% filter(service_point_sn == '3004478908')  # weekly consumption
Forecast <- function(my_data, model = NULL, specify.model = NULL){
  bl <<- my_data$service_point_sn[1]  # for testing
  
  period.ml <- 10  # (previous 9 weeks)
  n <- nrow(my_data)
  if (nrow(my_data) < (period.ml)*2+1){
    period.ml <- round(nrow(my_data)/2)}
  
  if (n < 10){
    return(NULL)
  }
    
    #####################
    # MACHINE  LEARNING #
    #####################  
    
    last_period <- function(nb,vect,period){
      return(vect[(nb:(nb-period+1))-1])
    }
    
    X.train <- foreach(j=(period.ml+1):n,.combine = 'rbind') %do% last_period(j,my_data$Consumption,period.ml)
    is.holiday.train_ml <- foreach(j=(period.ml+1):n,.combine = 'rbind') %do% last_period(j,my_data$Holidays,period.ml)
    X.train <- cbind(X.train,is.holiday.train_ml)
    Y.train <- my_data$Consumption[(period.ml+1):n]
    
    X.test <- last_period(n+1,my_data$Consumption,period.ml)
    is.holiday.test <- last_period(n,my_data$Holidays,period.ml)
    X.test <- c(X.test,is.holiday.test)
    
      rf.model <- try(suppressWarnings(randomForest(X.train,Y.train)),silent = TRUE)
      pred_rf <- ifelse(class(rf.model)=='try-error',NA,predict(rf.model,X.test))
   
      svm.model <- try(suppressWarnings(svm(X.train,Y.train,type='eps-regression')),silent = TRUE)
      pred_svm <- ifelse(class(svm.model)=='try-error',NA,predict(svm.model,t(as.matrix(X.test)))) 
      
      pred <- mean(c(pred_rf,pred_svm),na.rm=TRUE)
      
      # Option: neural network Time series forecast
      # if(sum(my_data$Consumption)==0){pred <- 0
      # }else{
      #   nn <- nnetar(my_data$Consumption)
      #   pred_nn <- forecast(nn)
      #   pred <- pred_nn$mean[1]
      # }
    res <- data.frame(service_point_sn = my_data$service_point_sn[1], week = Week.date$week[match(my_data$week[n],Week.date$week)+1],Forecast.Consumption = pred)
    return(res)
  
  # } else {
  #   return(NULL) 
  #}
}

week2predict <- paste(year(Sys.Date()),"_",strftime(Sys.Date()-7,format="%W"),sep="")

#if (week2predict=="2018_52") {week2predict <- "2017_52"}
if (week2predict==paste(year(today()),"_52",sep="")) {week2predict <- paste(year(today())-1,"_52",sep="")}

#week2predict <- '2017_08'  # start at week 2016_43
startforecast <- Week.date$beg[match(week2predict,Week.date$week)]
Historical_value <- Indiv %>% filter(Date < startforecast)
Value2predict <- Indiv %>% filter(week == week2predict) %>% select(service_point_sn,week,Consumption)

Forecast_Consumption <- plyr::ddply(Historical_value,.(service_point_sn),.fun = Forecast,specify.model = EFF,.progress = 'text')
# Forecast_Consumption <- plyr::ddply(Historical_value,.(service_point_sn),.fun = Forecast,model = 'ML_RF',.progress = 'text')
Forecast_Consumption$week_number <- week2predict 
Forecast_Consumption <- inner_join(Value2predict,Forecast_Consumption)

# Forecast.Consumption=0 when Consumption=0
Forecast_Consumption[Forecast_Consumption$Consumption==0,] %<>% mutate(Forecast.Consumption = 0)

Forecast_Consumption$Forecast.Consumption <- round(Forecast_Consumption$Forecast.Consumption)
Forecast_Consumption$error <- round(abs(Forecast_Consumption$Forecast.Consumption-Forecast_Consumption$Consumption)/Forecast_Consumption$Consumption,3)
