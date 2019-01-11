require(pacman)
pacman::p_load(xml2,rvest,stringr,dplyr,lubridate,foreach)
Extract.Weather <- function(start){
    aero <- 'WSSS'
    start.weather <- start
    today <- Sys.Date()
    time.weather <- data.frame(Date=seq(floor_date(start,'month'),floor_date(today,'month'),by='months'))
    time.weather$y <- year(time.weather$Date)
    time.weather$m <- month(time.weather$Date)
    time.weather$Date <- NULL
    time.weather <- distinct(time.weather)
    Gather.weather.info <- function(i){
        urlmeteo <-paste("http://wunderground.com/history/airport/",aero,"/",time.weather$y[i],"/",time.weather$m[i],"/1/MonthlyHistory.html?&reqdb.zip=&reqdb.magic=&reqdb.wmo=&format=1",sep="")
        page <- read_html(urlmeteo)
        meteo <- html_text(page)
        meteo <- str_split(meteo,'\n')
        # ligne <- unlist(strsplit(page,"<br />\n"))
        names <- unlist(strsplit(meteo[[1]][1],",")) 
        extract.weather.info <- function(i){
          unlist(strsplit(meteo[[1]][i],","))
        }
        DF <- t(sapply(2:length(meteo[[1]]),extract.weather.info))
        setTxtProgressBar(pb, i/nrow(time.weather))
        # cat(paste(dim(DF),collapse = "|"),'\n')
        return(DF)
    }
    
    pb <- txtProgressBar(min = 0, max = 1, style = 3)
    DF <- foreach(i = 1:nrow(time.weather),.combine = rbind,.packages = c('xml2','rvest')) %do% Gather.weather.info(i)
    close(pb)
    # names.col <- names[c(1:4,20)]
    names.col <- c('Date','Tmax','Tavg','Tmin','Rainf')
    Weather <- data.frame(DF[,c(1:4,20)])
    colnames(Weather) <- names.col
    Weather$Date <- ymd(Weather$Date)
    for (i in 2:ncol(Weather)){Weather[,i] <- as.numeric(as.character(Weather[,i]))}
    return(Weather)
}
