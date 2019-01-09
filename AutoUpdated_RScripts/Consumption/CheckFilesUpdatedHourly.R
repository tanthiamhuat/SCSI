rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(dplyr,lubridate,mailR,RPushbullet)

library(mailR)

setwd("/srv/shiny-server/DataAnalyticsPortal/data")
finf <- file.info(dir(), extra_cols = FALSE)
time_file1 <- finf[which(row.names(finf)=="Punggol_last30days.fst"),4]
time_file2 <- finf[which(row.names(finf)=="Punggol_thisyear.fst"),4]
 
time_diff1 <- as.numeric(abs(difftime(time_file1,Sys.time(),units = "mins")))
time_diff2 <- as.numeric(abs(difftime(time_file2,Sys.time(),units = "mins")))

if (time_diff1 > 80 | time_diff2 > 80){
  send.mail(from = "shd-snfr-autoreport@suez.com",
            to = c("thiamhuat.tan@suez.com"),
            subject = "Hourly Files Not Updated Properly",
            body = 'The hourly updated files (Punggol_last30days.fst & Punggol_thisyear.fst) are not updated properly on an hourly basis.
                    Please rectify it. It is very important for those files to be updated on an
                    hourly basis.',
            html = TRUE,
            inline = TRUE,
            smtp = list(host.name = "smtp.office365.com",
                        port = 587,
                        user.name = "thiamhuat.tan@suez.com",
                        passwd = "Joy03052007####",
                        tls = TRUE),
            authenticate = TRUE,
            send = TRUE)

  pbPost("note", title="Hourly Files Not Updated Properly",
         apikey = 'o.H7NrEOvUyppqW3T7L0J1zBpgsp4TDgdg')
}

load("/srv/shiny-server/DataAnalyticsPortal/data/DB_Consumption.RData")
time_diff4 <- as.numeric(abs(difftime(max(DB_Consumption$date_consumption),Sys.time(),units = "hours")))

if (time_diff4 > 6){
  send.mail(from = "shd-snfr-autoreport@suez.com",
            to = c("thiamhuat.tan@suez.com"),#"benjamin.evain@suez.com","shum.lik@suez.com"),
            subject = "Hourly Consumption Files Not Updated Properly",
            body = 'The hourly consumption files are not updated properly.
                    The time difference between its latest consumption data and the current hour
                    is more than 6 hours. Please verify its data from the database.',
            html = TRUE,
            inline = TRUE,
            smtp = list(host.name = "smtp.office365.com",
                        port = 587,
                        user.name = "thiamhuat.tan@suez.com",
                        passwd = "Joy03052007####",
                        tls = TRUE),
            authenticate = TRUE,
            send = TRUE)
  
  pbPost("note", title="Hourly Consumption Files Not Updated Properly",
         apikey = 'o.H7NrEOvUyppqW3T7L0J1zBpgsp4TDgdg') 
}

time_taken <- proc.time() - ptm
ans <- paste("CheckFilesUpdatedHourly successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)