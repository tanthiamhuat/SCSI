local_path <- 'D:\\DataAnalyticsPortal\\'
server_path <- '/srv/shiny-server/DataAnalyticsPortal/'  
path = local_path   

### Packages
# Install function for packages       
packages<-function(x){
  x<-as.character(match.call()[[2]])
  if (!require(x,character.only=TRUE)){
    install.packages(pkgs=x,repos="http://cran.r-project.org",dependencies = c("Depends"))
    require(x,character.only=TRUE)
  }
}
require(pacman)
# Shiny
pacman::p_load(shiny,shinyBS,shinydashboard,shinyjs)
# dataframe management
pacman::p_load(plyr,dplyr,tidyr,datasets,reshape2,data.table,DT,xlsx)
# Date management
pacman::p_load(xts,lubridate,readxl,zoo)
# Graphics
pacman::p_load(dygraphs,ggplot2,googleVis,plotly,flexdashboard)
# Maps
pacman::p_load(leaflet,sp,RgoogleMaps); #packages(maptools)
# others
pacman::p_load(htmlwidgets,digest,RPostgreSQL,scales,foreign,formattable,stringr,ISOweek,
               rhandsontable,D3TableFilter,fst,rpivotTable,pivottabler,geosphere,ipify,pivottabler,waterfalls,highcharter,gdata)

###
# /!\ only mandatory for french users so Weekdays appear as 'Mon.', 'Tue.', etc.
Sys.setlocale("LC_TIME", "C");
Sys.setenv(TZ='Asia/Singapore')

PASSWORD1 <- data.frame(UserName = "pubsuez", Password = digest('DAPj@2zP8Fx',algo = 'md5',serialize=F))
PASSWORD2 <- data.frame(UserName = "administrator", Password = digest('NBA6v&_L#H',algo = 'md5',serialize=F))

Logged = FALSE

Public.Holidays <- read.csv2(paste0(path,'data/Holidays_2014_2017.csv'),header = FALSE)
Public.Holidays$Date <- as.Date(Public.Holidays$V2,format='%d/%m/%Y')
Public.Holidays$Y <- year(Public.Holidays$Date)

Public.Holidays <- Public.Holidays %>% group_by(Y,V1) %>% dplyr::summarise(Start=min(Date),End=max(Date))
Public.Holidays$Duration <-  as.numeric(difftime(Public.Holidays$End,Public.Holidays$Start,units='days'))
Public.Holidays <- as.data.frame(Public.Holidays)

School.Holidays <- readxl::read_excel(paste0(path,'data/SchoolHolidays_2014_2017.xlsx'))
for (i in 1:nrow(School.Holidays)){
  if(is.na(School.Holidays$Start[i])){
    if(min(abs(difftime(ymd(School.Holidays$End[i]),Public.Holidays$End)))==1){
        x.temp <- which(abs(difftime(School.Holidays$End[i],Public.Holidays$End))==1)
        Public.Holidays$End[x.temp] <- School.Holidays$End[i]
        Public.Holidays$Duration[x.temp] <- Public.Holidays$Duration[x.temp]+1
      }else{
        Public.Holidays <- rbind(Public.Holidays,data.frame(Y=year(School.Holidays$End[i]),V1=School.Holidays$Meaning[i],Start=School.Holidays$End[i],End=School.Holidays$End[i],Duration=0))
      }
  }else{
    temp <- data.frame(Y=year(ymd(as.character(School.Holidays$Start[i]))),
                       V1 = School.Holidays$Meaning[i],
                       Start = ymd(as.character(School.Holidays$Start[i])),
                       End = ymd(as.character(School.Holidays$End[i])))
    temp$Duration <- as.numeric(difftime(temp$End,temp$Start,units = 'days'))
    Public.Holidays <- rbind(Public.Holidays,temp)
  }  
}

#suppressWarnings(Customer <- read_excel('/srv/shiny-server/DataAnalyticsPortal/data/PunggolReportFullBlocks v3.xlsx',sheet = 1,skip = 1))
suppressWarnings(Customer <- read_excel(paste0(path,'data/PunggolReportFullBlocks v3.xlsx'),sheet = 1,skip = 1))
