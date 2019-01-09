rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

###########################
# LOADING PACKAGES        #
###########################
if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(foreach,randomForest,e1071,plyr,dplyr,ggplot2,lubridate,doSNOW,parallel,snow,boot,stats,xts,forecast)

holidays <- read.csv("/srv/shiny-server/DataAnalyticsPortal/data/Holidays_2014_2017.csv",sep = ";",header = FALSE)
colnames(holidays) <- c("Holidays","Date")
holidays$Date <- dmy(holidays$Date)
##########################
# LOADING DATA           #
##########################
load("/srv/shiny-server/DataAnalyticsPortal/data/Week.date.RData")
Punggol_All <- read.fst("/srv/shiny-server/DataAnalyticsPortal/data/Punggol_last12months.fst")

X <- Punggol_All %>% dplyr::filter(room_type != 'NIL', adjusted_consumption != 'NA') # contain data on sub meter, including childcare

X <- X %>% filter(Date >= ymd('2016-02-29'))%>% filter(room_type!='HDBCD') %>%
  group_by(service_point_sn,Date,wd,week,room_type,block) %>% dplyr::summarise(Consumption = sum(Consumption))
## X: Consumption for each day
X$holiday <- (X$Date %in% holidays$Date) *1

Indiv <- X %>% group_by(service_point_sn,week) %>% dplyr::summarise(Consumption=sum(Consumption),
                                                                    Holidays=sum(holiday)) 
# Consumption for each week
Indiv$Date <- Week.date$beg[match(Indiv$week,Week.date$week)]
nb.bootstrap<- 20

# my_data <- Indiv %>% filter(service_point_sn == sn)  # testing

Efficiency_forecast <- function(my_data,nb.bootstrap = nb.bootstrap){
  sn <<- my_data$service_point_sn[1]
  period.ml <- 3  # machine learning (last 3 weeks to predict the next week)
  if (nrow(my_data)>(period.ml)*2+1){
  
  bootstrap.period <- sample(((period.ml)*2+1):nrow(my_data),nb.bootstrap,replace = TRUE) # set TRUE for dataframe having not enough values
  bootstrap.period.save <<-  bootstrap.period 
  bootstrap.period  <- bootstrap.period.save
  Forecast.model <- function(n){
    #n.save <<- n
    
    #####################
    # MACHINE  LEARNING #
    #####################
    val2predict <- my_data$Consumption[n]  
    week2predict <- my_data$week[n]
    
    last_period <- function(nb,vect,period){
      return(vect[(nb:(nb-period+1))-1])
    }
    
    X.train <- foreach(j=4:(n-1),.combine = 'rbind') %do% last_period(j,my_data$Consumption,period.ml)
    is.holiday.train_ml <- foreach(j=4:(n-1),.combine = 'rbind') %do% last_period(j,my_data$Holidays,period.ml)
    X.train <- cbind(X.train,is.holiday.train_ml)
    Y.train <- my_data$Consumption[4:(n-1)]
    
    X.test <- last_period(n,my_data$Consumption,period.ml)
    is.holiday.test <- last_period(n,my_data$Holidays,period.ml)
    X.test <- c(X.test,is.holiday.test)
    
    rf.model <- try(suppressWarnings(randomForest(X.train,Y.train)),silent = TRUE) # X.train: predictors, Y.train: response
    svm.model <- try(suppressWarnings(svm(X.train,Y.train,type='eps-regression')),silent = TRUE)
    pred.rf <- ifelse(class(rf.model)=='try-error',NA,predict(rf.model,X.test))
    pred.svm <- ifelse(class(svm.model)=='try-error',NA,predict(svm.model,t(as.matrix(X.test))))
    
    ###############
    # TIME SERIES #
    ###############
    period.ts <- 365.25/7
    my_xts <- ts(my_data$Consumption[(1:(n-1))],start = my_data$Date[1],frequency = period.ts)
    #xreg.train <- ts(my_data$holiday[(1:(n-1))],start = my_data$Date[1],frequency = period.ts)
    xreg.train <- my_data$Holidays[(1:(n-1))]
    xreg.test  <- my_data$Holidays[n]
    
    list_res <- list()
    #Simple exponential smoothing (without trend and without seasonal component)
    mod_HW <- try(HoltWinters(my_xts, alpha=NULL, beta = NULL, gamma = FALSE), silent = TRUE)
    list_res <- append(list_res, list(mod_HW))
   
    #ARIMAX Model
    mod_ARMAX <- try(auto.arima(my_xts,xreg = xreg.train), silent = TRUE)
    if('Arima' %in% class(mod_ARMAX) && !("xreg" %in% names(mod_ARMAX))){mod_ARMAX <- try(auto.arima(my_xts), silent = TRUE)}
    list_res<-append(list_res, list(mod_ARMAX))
    
    predict.ts <- function(model){
      if(!"try-error" %in% class(model)){
        if('Arima' %in% class(model)){
          if("xreg" %in% names(model)){
            return(as.numeric(predict(model,newxreg=xreg.test)$pred[1])) 
          }else{return(as.numeric(predict(model)$pred[1])) }
        }else{
          return(as.numeric(predict(model)))
        }
      }else{return(NA)}
    }
    # xreg.test <- is.week.end[n]
    pred.ts <- sapply(list_res, predict.ts)
    if (is.list(pred.ts)) pred.ts <- unlist(pred.ts)
    pred.value <- c(pred.rf,pred.svm,pred.ts)
    error <- (pred.value-val2predict)/val2predict
    if(val2predict==0){
      bias <- 1/10000
      error <- ((pred.value-bias)-(val2predict-bias))/(val2predict-bias)
      # error[which(pred.value==0)] <- 0
    }
    return(error)
  }
  
  nb_cores <- detectCores()-1
  cl <- makeSOCKcluster(rep("localhost", nb_cores))
  registerDoSNOW(cl)
  res_model <- foreach(i=bootstrap.period, .combine=rbind, .packages = c("foreach","lubridate","forecast","randomForest","e1071")) %dopar% {Forecast.model(i)}
  stopCluster(cl)
  
  res <- as.data.frame(t(colMeans(abs(res_model),na.rm = TRUE)))
  model.name <- c("ML_RF","ML_SVM","TS_HW","TS_ARIMA")
  
  colnames(res) <- model.name
  res_final <- cbind(my_data$service_point_sn[1],res)
  colnames(res_final) <- c('service_point_sn',model.name)
  return(res_final)
  
  } else {
  return(NULL)  
  }
}

# efficiency of forecast model for each customer
EFF <- ddply(Indiv,.(service_point_sn),.fun = Efficiency_forecast,nb.bootstrap,.progress = 'text')

# q(save = 'no')

temp <- EFF[,-1]
best.model <- function(x){
  names(which.min(x))
}
EFF$bestmodel <- apply(X = temp,MARGIN = 1, FUN = best.model)
save(EFF,file = '/srv/shiny-server/DataAnalyticsPortal/data/Select Forecast Model_Weekly_Indiv_Consumption.RData')
