# http://openweathermap.org/current
# https://home.openweathermap.org/api_keys
# temperature is in Kelvin, minus 273.15 to get deg C

rm(list=ls())  # remove all variables
cat("\014")    # clear Console
if (dev.cur()!=1) {dev.off()} # clear R plots if exists

library(XML)
library(httr)
library(magrittr)
library(jsonlite)

query <- "http://api.openweathermap.org/data/2.5/forecast?id=1880252&APPID=635ed52077dc02fd5e1cff26f6d3d9bd"

document <- fromJSON(query)
WeatherForecast <- as.data.frame(document$list)
