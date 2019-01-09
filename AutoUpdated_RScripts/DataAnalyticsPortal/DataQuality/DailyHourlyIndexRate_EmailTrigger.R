rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(dplyr,lubridate,mailR)

library(mailR,RPushbullet)

options(warn=-1)  # suppress all warnings

load("/srv/shiny-server/DataAnalyticsPortal/data/DailyHourlyIndexRate_last30days.RData")

last2day_Rates <- HourlyIndexReadingRate %>% 
                  dplyr::filter(date==today()-2) %>%  
                  select(HourlyIndexReadingRate)

if (last2day_Rates$HourlyIndexReadingRate < 80) {
  ## how to set mail priority as important?
  send.mail(from = "shd-snfr-autoreport@suez.com",
            to = c("thiamhuat.tan@suez.com"),
            subject = "Hourly Index Rate < 80%",
            body = 'The Hourly Index Rate drops to less than 80%. 
                    Please verify its data from the database.',
            html = TRUE,
            inline = TRUE,
            smtp = list(host.name = "smtp.office365.com",
                        port = 587,
                        user.name = "thiamhuat.tan@suezenvironnement.com",
                        passwd = "Joy@03052007####",
                        tls = TRUE),
            authenticate = TRUE,
            send = TRUE)
}

time_taken <- proc.time() - ptm
ans <- paste("DailyHourlyIndexRate_EmailTrigger successfully completed in",round(time_taken[3],2),"seconds.")
print(ans)