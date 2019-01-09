RawConsumptionFiltered_16March <- RawConsumption %>% 
  filter(date(ReadingDate)=="2017-03-16" & 
           ExternalMeteringPointReference %in% unique(PunggolConsumption$service_point_sn))

AWSConsumptionFiltered_16March  <- DB_Consumption %>% 
  filter(date(date_consumption)=="2017-03-16" & 
           id_service_point %in% unique(family_servicepoint$id_service_point)) 

AWSConsumptionFiltered_16March_SP <- inner_join(AWSConsumptionFiltered_16March,servicepoint,by=c("id_service_point"="id")) %>%
                                     dplyr::select_("service_point_sn","interpolated_consumption","adjusted_consumption","date_consumption")

RawAWS_16March <- inner_join(RawConsumptionFiltered_16March,AWSConsumptionFiltered_16March_SP,
                             by=c("ExternalMeteringPointReference"="service_point_sn","ReadingDate"="date_consumption")) %>%
                  dplyr::select_("ExternalMeteringPointReference","interpolated_consumption","adjusted_consumption","ReadingDate") %>%
                  dplyr::rename(service_point_sn=ExternalMeteringPointReference,date_consumption=ReadingDate)

setdiffDF <- function(A, B){ 
  f <- function(A, B) 
    A[!duplicated(rbind(B, A))[nrow(B) + 1:nrow(A)], ] 
  df1 <- f(A, B) 
  df2 <- f(B, A) 
  rbind(df1, df2) 
} 

ExtraAWS <- setdiffDF(AWSConsumptionFiltered_16March_SP, RawAWS_16March) 
