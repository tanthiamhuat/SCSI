rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

ptm <- proc.time()

pacman::p_load(mailR,lubridate)

subject_contents = paste("Data Quality Monitoring Report (Singapore)")

send.mail(from = "shd-snfr-autoreport@suez.com",
          to = c("thiamhuat.tan@suez.com","huiling.ang@suez.com","noemie.laurent@suez.com"),
          subject = subject_contents,
          body = "Please copy and paste the weblink \n
                  http://52.74.103.158/reports/DataQualityMonitoringReport.html \n
                  to a browser.
                  Username: pubsuez \n
                  Password: DAPj@2zP8Fx \n
                  This email was sent automatically. \n
                  Please do not reply directly to this e-mail.",
          html = TRUE,
          inline = TRUE,
          smtp = list(host.name = "smtp.office365.com",
                      port = 587,
                      user.name = "thiamhuat.tan@suez.com",
                      passwd = "Joy03052007#####",
                      tls = TRUE),
          authenticate = TRUE,
          send = TRUE)

time_taken <- proc.time() - ptm
ans <- paste("AutomatedEmail2Ondeo successfully completed in",round(time_taken[3],2),"seconds on",now())
print(ans)

cat(ans,'\n',file="/srv/shiny-server/DataAnalyticsPortal/data/log.txt",append=TRUE)

