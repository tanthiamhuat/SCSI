rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

ptm <- proc.time()

library(httr)
library(jsonlite)
library(lubridate)

## start of month
som <- function(x) {
  as.Date(format(x, "%Y-%m-01"))
}

## end of month
eom <- function(x) {
  som(som(x) + 35) - 1
}

end_month <- eom(Sys.Date())
month_extracted <- substr(Sys.Date(),6,7)
  
url <- paste("https://api-ak.wunderground.com/api/d8585d80376a429e/history_",year(today()),month_extracted,"01",gsub("-", "", end_month),
              "/lang:EN/units:english/bestfct:1/v:2.0/q/WSSS.json?showObs=0&ttl=120",sep="")

#url <- 'https://api-ak.wunderground.com/api/d8585d80376a429e/history_2018120120181215/lang:EN/units:english/bestfct:1/v:2.0/q/WSSS.json?showObs=0&ttl=120'
#url <- 'https://api-ak.wunderground.com/api/d8585d80376a429e/history_20181215/lang:EN/units:english/bestfct:1/v:2.0/q/WSSS.json?showObs=0&ttl=120'  ## one date

DateStart <- substr(url,62,69)

r <- GET(url)
r$status_code
responseContent <- content(r, "text")
json <- fromJSON(responseContent)
a=as.data.frame(unlist(json),stringsAsFactors = FALSE) 

ndays <- 2+(nrow(a)-330)/85

Days <- as.data.frame(seq.Date(from=as.Date(DateStart,"%Y%m%d"),by="days",length.out=ndays))
colnames(Days) <- "Date"

MaxTemp <- list()
AvgTemp <- list()
MinTemp <- list()
RainFall <- list()
for (i in 1:length(seq(1,round(ndays)))){
  MaxTemp[i] <- paste("history.days.summary.max_temperature",i,sep="")
  AvgTemp[i] <- paste("history.days.summary.temperature",i,sep="")
  MinTemp[i] <- paste("history.days.summary.min_temperature",i,sep="")
  RainFall[i] <- paste("history.days.summary.precip",i,sep="")
}

variables <- unlist(c(MaxTemp,AvgTemp,MinTemp,RainFall))

a1 = a[variables,]

a2 = as.data.frame(as.double(a1),stringsAsFactors = FALSE)
colnames(a2) <- "data"
TempRF <- as.data.frame(matrix(c(a2$data), nrow = round(ndays), ncol = 4))
TempRF[,1:3] <- round((TempRF[,1:3]-32)*(5/9),2) # convert from Fahrenheit to Celsius
TempRF[,4] <- round(TempRF[,4]*25.4,2) #convert from (in) to mm

colnames(TempRF) <- c("Tmax","Tavg","Tmin","Rainf")

DaysTempRF <- cbind(Days,TempRF)
weather_new <- DaysTempRF[nrow(DaysTempRF):nrow(DaysTempRF),]

load(paste0(path,'data/weather_updated.RData'))
weather_updated <- rbind(weather_updated,weather_new)
rownames(weather_updated) <- seq(1,nrow(weather_updated))

save(weather_updated,file=paste0(path,'data/weather_updated.RData'))

load(paste0(path,'data/Weather_2016.RData'))
load(paste0(path,'data/Weather_2017.RData'))
Weather <- rbind(rbind(weather_2016,weather_2017),weather_updated)
rownames(Weather) <- seq(1,nrow(Weather))
save(Weather,file=paste0(path,'data/Weather.RData'))

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_WeatherScrapping successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file=paste0(path,'data/log_DT.txt'),append=TRUE)

