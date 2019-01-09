rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'
path = server_path

ptm <- proc.time()

if(!'pacman' %in% installed.packages()){
  install.packages('pacman')
}
require(pacman)
pacman::p_load(ISOweek,lubridate)

load(paste0(path,'data/Week.date.RData'))

todayweeknumber <- as.numeric(strftime(today(),format="%W")) 
  if (todayweeknumber <10) {todayweeknumber <- paste(0,todayweeknumber,sep="")}
  week <- paste(year(Sys.Date()),"-W",todayweeknumber,sep="") 
  beg <- ISOweek2date(paste(week,1,sep="-"))
  end <- ISOweek2date(paste(week,7,sep="-"))
  week <- gsub("-W", "_", week) # replace -W with _
Week.date.new <- data.frame(week,beg,end)
Week.date <- rbind(Week.date,Week.date.new)
save(Week.date,file=paste0(path,'data/Week.date.RData'))

time_taken <- proc.time() - ptm
ans <- paste("AutoUpdated_Week.date successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file=paste0(path,'data/log.txt'),append=TRUE)
