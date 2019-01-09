###########################
# LOADING PACKAGES        #
###########################
if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(DBI,RPostgreSQL,foreach,randomForest,e1071,plyr,dplyr,magrittr,ggplot2,lubridate,doSNOW,parallel,snow,boot,stats,xts,forecast)

source('/srv/shiny-server/DataAnalyticsPortal/AutoUpdated_RScripts/DB_Connections.R')

holidays <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/Holidays_2014_2017.csv",sep = ";",header = FALSE)
colnames(holidays) <- c("Holidays","Date")
holidays$Date <- dmy(holidays$Date)

family <- as.data.frame(tbl(con,"family")) %>% 
  dplyr::filter(pub_cust_id!="EMPTY" & status=="ACTIVE" & !(room_type %in% c("MAIN","BYPASS","HDBCD")))

servicepoint <- as.data.frame(tbl(con,"service_point")) %>% dplyr::filter(service_point_sn !="3100507837M" & service_point_sn != "3100507837B")
family_servicepoint <- inner_join(family,servicepoint,by=c("id_service_point"="id"))

##########################
# LOADING DATA           #
##########################
load("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_Final_DF_V2.RData")
X <- Punggol_All %>% dplyr::filter(room_type != 'NIL', adjusted_consumption != 'NA') # contain data on sub meter, including childcare
rm(Punggol_All)

X <- X %>% filter(Date >= ymd('2016-02-29'))%>% filter(room_type!='HDBCD')
X$holiday <- (X$Date %in% holidays$Date)*1

X1 <- inner_join(X,family_servicepoint) %>%
      dplyr::filter(date(date_consumption) >= move_in_date)  # take into consideration of move-in-date
                                                           
Block <- X1 %>% group_by(block,week) %>% dplyr::summarise(Consumption=sum(Consumption),
                                                                    Holidays=sum(holiday)) 

# weekly consumption
Block$Date <- Week.date$beg[match(Block$week,Week.date$week)]

load("/srv/shiny-server/DataAnalyticsPortal/data/Select Forecast Model_Weekly_Block_Consumption.RData")

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

# my_data <- Indiv %>% filter(service_point_sn == '3100660792')  # weekly consumption
Forecast <- function(my_data, model = NULL, specify.model = NULL){
  bl <<- my_data$block[1]  # for testing
  if(is.null(specify.model) & !is.null(model)){
    if(!model %in% c("ML_RF","ML_SVM","TS_HWexpS","TS_HW","TS_ARIMA")){
      stop('Please enter one of the following model:\n("ML_RF","ML_SVM","TS_HWexpS","TS_HW")')
    }
  }
  if(is.null(model)){model <- "ML_RF"}
  final.model <- ifelse(is.null(specify.model) | is.na(match(my_data$block[1],specify.model$block)), 
                        model, specify.model$bestmodel[match(my_data$block[1],specify.model$block)])
  type.model <- substr(final.model,1,2)
  
  period.ml <- 3  # (previous 3 weeks)
  n <- nrow(my_data)
  if (nrow(my_data) > (period.ml)*2+1){
    
  if(type.model=='ML'){
    #####################
    # MACHINE  LEARNING #
    #####################  
    
    # period.learning <- 3 # we use the last 7 values/days for the TS models
    last_period <- function(nb,vect,period){
      return(vect[(nb:(nb-period+1))-1])
    }
    
    X.train <- foreach(j=4:n,.combine = 'rbind') %do% last_period(j,my_data$Consumption,period.ml)
    is.holiday.train_ml <- foreach(j=4:n,.combine = 'rbind') %do% last_period(j,my_data$Holidays,period.ml)
    X.train <- cbind(X.train,is.holiday.train_ml)
    Y.train <- my_data$Consumption[4:n]
    
    X.test <- last_period(n+1,my_data$Consumption,period.ml)
    is.holiday.test <- last_period(n,my_data$Holidays,period.ml)
    X.test <- c(X.test,is.holiday.test)
    
    if(final.model=="ML_RF"){
      rf.model <- try(suppressWarnings(randomForest(X.train,Y.train)),silent = TRUE)
      pred <- ifelse(class(rf.model)=='try-error',NA,predict(rf.model,X.test))
    }else{
      svm.model <- try(suppressWarnings(svm(X.train,Y.train,type='eps-regression')),silent = TRUE)
      pred <- ifelse(class(svm.model)=='try-error',NA,predict(svm.model,t(as.matrix(X.test)))) 
    }
  }else{ 
    ###############
    # TIME SERIES #
    ###############  
    period.ts <- 365.25/7
    my_xts <- ts(my_data$Consumption[(1:n)],start = my_data$Date[1],frequency = period.ts)
    xreg.train <- my_data$Holidays[(1:n)]
    xreg.test  <- my_data$Holidays[n]
    
    if(final.model=='TS_HW'){
      ts.model <- try(HoltWinters(my_xts, alpha=NULL, beta = NULL, gamma = FALSE), silent = TRUE)
      pred <- ifelse(class(ts.model) != "try-error", as.numeric(predict(ts.model)), NA)
    }
    if(final.model=='TS_ARIMA'){
      ts.model <- try(auto.arima(my_xts,xreg = xreg.train), silent = TRUE)
      if(!"try-error" %in% class(ts.model) && !("xreg" %in% names(ts.model))){
        ts.model <- try(auto.arima(my_xts,allowdrift = FALSE), silent = TRUE)
        pred <- ifelse(!"try-error" %in% class(ts.model), as.numeric(predict(ts.model)$pred), NA)
      }else{
        pred <- ifelse(!"try-error" %in% class(ts.model), as.numeric(predict(ts.model,newxreg=xreg.test)$pred), NA)
      }
    }
    ## if predict consumption less than zero, we use Random Forest ML.
    if (is.na(pred) || pred < 0) {
      last_period <- function(nb,vect,period){
        return(vect[(nb:(nb-period+1))-1])
      }
      
      X.train <- foreach(j=4:n,.combine = 'rbind') %do% last_period(j,my_data$Consumption,period.ml)
      is.holiday.train_ml <- foreach(j=4:n,.combine = 'rbind') %do% last_period(j,my_data$Holidays,period.ml)
      X.train <- cbind(X.train,is.holiday.train_ml)
      Y.train <- my_data$Consumption[4:n]
      
      X.test <- last_period(n+1,my_data$Consumption,period.ml)
      is.holiday.test <- last_period(n,my_data$Holidays,period.ml)
      X.test <- c(X.test,is.holiday.test)

      rf.model <- try(suppressWarnings(randomForest(X.train,Y.train)),silent = TRUE)
      pred <- ifelse(class(rf.model)=='try-error',NA,predict(rf.model,X.test))
      final.model <- "ML_RF"
    }
  }
  res <- data.frame(block = my_data$block[1], week = Week.date$week[match(my_data$week[n],Week.date$week)+1],Model = final.model, Forecast.Consumption = pred)
  return(res)
  
  } else {
    return(NULL) 
  }
}

week2predict <- paste(year(Sys.Date()),"_",strftime(Sys.Date()-7,format="%W"),sep="")
if (week2predict=="2017_52") {week2predict <- "2016_52"}
#week2predict <- '2016_48'  # start at week 2016_43
startforecast <- Week.date$beg[match(week2predict,Week.date$week)]
Historical_value <- Block %>% filter(Date < startforecast)
Value2predict <- Block %>% filter(week == week2predict) %>% select(block,week,Consumption)

Forecast_Consumption <- plyr::ddply(Historical_value,.(block),.fun = Forecast,specify.model = EFF,.progress = 'text')
Forecast_Consumption$week_number <- week2predict 
Forecast_Consumption <- inner_join(Value2predict,Forecast_Consumption)

# Forecast.Consumption=0 when Consumption=0
Forecast_Consumption[Forecast_Consumption$Consumption==0,] %<>% mutate(Forecast.Consumption = 0)

Forecast_Consumption$Forecast.Consumption <- round(Forecast_Consumption$Forecast.Consumption)
Forecast_Consumption$error <- round(abs(Forecast_Consumption$Forecast.Consumption-Forecast_Consumption$Consumption)/Forecast_Consumption$Consumption,3)