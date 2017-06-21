fRefreshData <- function(rawdata.dir, savedata.dir, surveys, 
                         stock = "MAC-NEA", startyear=2003, endyear=2015, 
                         refresh = FALSE){
  
  require(dplyr)
  
  to.save <- c()
  
  if (refresh) {
    
    #IGFS
    if ("IE-IGFS" %in% surveys) {
      dfIGFS <- floadIGFSfromDATRAS(survey = "IE-IGFS",
                                    startyear = startyear,
                                    endyear = endyear,
                                    quarters=c(1,2,3,4))
      to.save <- c(to.save,"dfIGFS")
      dfHauls <- dfIGFS
    }
    

    cat("saving to SourceGF.RData\n")  
    save(to.save,file=paste0(savedata.dir,"/",stock,"_SourceGF.RData"))
    
    #select the variables of interest
    dfHauls <- dplyr::select(dfHauls,HOM,HOM_unrd,Juv,YC,Year,Quarter,StatRec,
                             Depth,Age0,Age0_unrd,Age1Plus,Age1Plus_unrd,Fishable,Fishable_unrd,
                             HaulNo,MidLat,MidLon,Survey,GroundSpeed,WingSpread,HaulDur,Netopening,Distance,
                             Country,Ship,Gear,CatchWgt,RdCatchWgtKg,SweptArea_m2,SweptArea_nm2,SweptVolume_m3,
                             Num1cm,Num2cm,Num3cm,Num4cm,Num5cm,Num6cm,Num7cm,Num8cm,Num9cm,Num10cm,
                             Num11cm,Num12cm,Num13cm,Num14cm,Num15cm,Num16cm,Num17cm,Num18cm,Num19cm,Num20cm,
                             Num21cm,Num22cm,Num23cm,Num24cm,Num25cm,Num26cm,Num27cm,Num28cm,Num29cm,Num30cm,
                             Num31cm,Num32cm,Num33cm,Num34cm,Num35cm,Num36cm,Num37cm,Num38cm,Num39cm,Num40cm,
                             Num41cm,Num42cm,Num43cm,Num44cm,Num45cm,Num46cm,Num47cm,Num48cm,Num49cm,Num50cm,
                             Num51cm,Num52cm,Num53cm,Num54cm,Num55cm,Num56cm,Num57cm,Num58cm,Num59cm,Num60cm)
    
    #save reformated data
    save(dfHauls, file=paste0(savedata.dir,"/",stock,"_CleanGF.RData"))
    
  } else {
    
    #load in data
    load(file=paste0(savedata.dir,"/",stock,"_CleanGF.RData"))
    
  }
  
  dfHauls
  
}


floadIGFSfromDATRAS <- function(survey="IE-IGFS", startyear=2003, endyear=2015, 
                                quarters=c(1,2,3,4)) {
  
  #read IGFS data from DATRAS
  
  #haul records
  haulData <- getDATRAS(record="HH",
                        survey=survey,
                        startyear=startyear,
                        endyear=endyear,
                        quarters=quarters,
                        parallel=TRUE,
                        cores=4)
  
  haulData <- as.data.frame(haulData)[,-1]
  
  cat(dim(haulData),"\n")
  cat(names(haulData),"\n")
  
  #only valid hauls with depths>0 considered
  haulData <- dplyr::filter(haulData, toupper(stringr::str_trim(HaulVal)) == "V")
  haulData <- dplyr::filter(haulData, as.numeric(stringr::str_trim(Depth))>0)
  
  haulData$Year <- as.numeric(haulData$Year)
  haulData$Quarter <- as.numeric(haulData$Quarter)
  haulData$GroundSpeed <- as.numeric(haulData$GroundSpeed)
  haulData$WingSpread <- as.numeric(haulData$WingSpread)
  haulData$ShootLat <- as.numeric(haulData$ShootLat)
  haulData$ShootLong <- as.numeric(haulData$ShootLong)
  haulData$HaulLat <- as.numeric(haulData$HaulLat)
  haulData$HaulLong <- as.numeric(haulData$HaulLong)
  haulData$HaulDur <- as.numeric(haulData$HaulDur)
  haulData$Netopening <- as.numeric(haulData$Netopening)
  
  haulData <- dplyr::select(haulData,Year,Quarter,StatRec,StNo,HaulNo,HaulDur,ShootLat,ShootLong,
                            HaulLat,HaulLong,Depth,HaulVal,Netopening,Distance,
                            WingSpread,GroundSpeed,Country,Ship,Gear)
  
  
  #replace -9 values for GroundSpeed and WingSpread with the mean of the annual values
  for (y in names(table(haulData$Year))){
    haulData$WingSpread[haulData$WingSpread==-9 & haulData$Year==y] <- mean(haulData$WingSpread[haulData$WingSpread>0 & haulData$Year==y])
    haulData$GroundSpeed[haulData$GroundSpeed==-9 & haulData$Year==y] <- mean(haulData$GroundSpeed[haulData$GroundSpeed>0 & haulData$Year==y])
  }
  
  #add SurvStratum col
  haulData <- dplyr::mutate(haulData, SurvStratum=NA)
  
  #add a yc column to track the last yc to enter the fishery. For q3 and q4 surveys this will be the same as the survey year
  #for q1 and q2 surveys it will be the year prior to the survey year
  haulData <- dplyr::mutate(haulData, YC = NA)
  haulData[haulData$Quarter %in% c(1,2),]$YC <- haulData[haulData$Quarter %in% c(1,2),]$Year - 1
  haulData[haulData$Quarter %in% c(3,4),]$YC <- haulData[haulData$Quarter %in% c(3,4),]$Year
  
  haulData$StatRec = paste0("SR",haulData$StatRec)
  
  #create a unique record identifier comprising year, quarter and haul number
  haulData$Idx <- paste(haulData$Year,":",haulData$Quarter,":",haulData$HaulNo,sep="")
  
  #midpoint of shoot/haul lat
  haulData$MidLat <- (haulData$ShootLat + haulData$HaulLat)/2
  haulData$MidLon <- (haulData$ShootLong + haulData$HaulLong)/2
  
  #check it's unique, sum should be zero
  cat(sum(table(haulData$Idx)>1),"\n")
  
  cat(dim(haulData),"\n")
  
  #length records
  lengthData <- getDATRAS(record="HL",
                          survey=survey,
                          startyear=startyear,
                          endyear=endyear,
                          quarters=quarters,
                          parallel=TRUE,
                          cores=4)
  
  lengthData <- as.data.frame(lengthData)[,-1]
  
  cat(names(lengthData),"\n")
  
  lengthData$Year <- as.numeric(lengthData$Year)
  lengthData$Quarter <- as.numeric(lengthData$Quarter)
  lengthData$SpecCode <- as.numeric(lengthData$SpecCode)
  lengthData$LngtClass <- as.numeric(lengthData$LngtClass)
  lengthData$SubFactor <- as.numeric(lengthData$SubFactor)
  lengthData$HLNoAtLngt <- as.numeric(lengthData$HLNoAtLngt)
  lengthData$CatCatchWgt <- as.numeric(lengthData$CatCatchWgt)
  
  lengthData <- filter(lengthData, (toupper(stringr::str_trim(SpecCodeType))=="W" & SpecCode==126822) | 
                         (toupper(stringr::str_trim(SpecCodeType))=="T" & SpecCode==168588))
  
  #length class data in mm and cm - convert all to cm
  lengthData$LngtClasscm <- lengthData$LngtClass
  #convert mm to cm
  lengthData$LngtClasscm[stringr::str_trim(lengthData$LngtCode) == "."] <- lengthData$LngtClass[stringr::str_trim(lengthData$LngtCode) == "."]/10
  
  #only positive vals
  lengthData <- filter(lengthData,LngtClasscm>0)
  
  #select variables of interest
  lengthData <- dplyr::select(lengthData,Year,Quarter,StNo,HaulNo,TotalNo,CatIdentifier,CatCatchWgt,SubFactor,LngtClasscm,HLNoAtLngt)
  
  #create a unique haul identifier
  lengthData$Idx <- paste(lengthData$Year,":",lengthData$Quarter,":",lengthData$HaulNo,sep="")
  
  #join with haul information
  lengthData <- dplyr::inner_join(lengthData,dplyr::select(haulData,HaulDur,YC,Idx),by="Idx")
  
  #calculate the total number at length 
  lengthData <- mutate(lengthData, NoAtLen = SubFactor*HLNoAtLngt)
  
  #add categories (where there is more than one sample for WHM in the haul)
  lengthData <- lengthData %>% group_by(Year,Quarter,HaulNo,Idx,YC,StNo,HaulDur,CatIdentifier,CatCatchWgt,LngtClasscm) %>% summarise(TotNoAtLen = sum(NoAtLen))
  
  #raised number at length (to 1 hour)
  lengthData <- mutate(lengthData, RdNoAtLen = TotNoAtLen*(60/as.numeric(HaulDur)))
  
  #mark as age0 true/false (depending on cutoff)
  #cutoff varies by year
  lengthData$Age0YN <- NA
  
  for (y in unique(lengthData$Year)){
    cat(y,"\n")
    LFCutOff <- dplyr::filter(dfLFCutOff,Survey=="IGFS" & Year==y & Quarter==4)$CutOff
    cat(y,LFCutOff,"\n")
    lengthData[lengthData$Year==y,]$Age0YN <- lengthData[lengthData$Year==y,]$LngtClasscm<=LFCutOff
  }
  
  cat(dim(lengthData),"\n")
  
  dfIGFS.LF <- data.frame(Len = seq(from = range(lengthData$LngtClasscm)[1], to = range(lengthData$LngtClasscm)[2]))
  
  #length frequency plot
  jpeg(filename=paste0(plot.dir,"/",survey,"_LF.jpg"), width=10, height=10, units="cm", res=72, quality=100)
  
  dfAllMeas <- lengthData %>% filter(YC>=2001) %>% group_by(LngtClasscm) %>% summarise(AllatLen = sum(RdNoAtLen)) %>% arrange(LngtClasscm)
  
  dfIGFS.LF$Num_all <- dfAllMeas$AllatLen
  
  barplot(dfAllMeas$AllatLen, names.arg = dfAllMeas$LngtClasscm, ylab="", xlab="Length (cm)", axes=F, main=survey)
  
  dev.off()
  
  
  #haul distribution and length frequency by year
  for (j in as.integer(names(table(lengthData$Year)))) {
    
    LFCutOff <- dplyr::filter(dfLFCutOff,Survey=="IGFS" & Year==j & Quarter==4)$CutOff
    
    #hauls    
    jpeg(filename=paste0(plot.dir,"/",survey,"_Hauls_",j,".jpg"), 
         width=800, 
         height=600, 
         quality=100)
    
    plot(seq(W.Lon,E.Lon,length=100),seq(S.Lat,N.Lat,length=100),type="n", xlab="Longitude", ylab="Latitude")
    
    #bathymetry
    if (sum(unlist(bathytoplot))>0) {
      for (i in which(bathytoplot==TRUE)){
        lines(bathymetry[[i]]$x,bathymetry[[i]]$y,col="grey")
      }}
    
    #coast  
    if (sum(unlist(coasttoplot))>0) {
      for (i in which(coasttoplot==TRUE)) {
        if (coast[[i]]$fill==TRUE) {
          polygon(coast[[i]]$Lon,coast[[i]]$Lat,col="grey")
        } else {
          polygon(coast[[i]]$Lon,coast[[i]]$Lat,col="white")             
        }}}
    
    with(dplyr::filter(haulData, Year==j), points(MidLon,MidLat,pch=20))
    #with(filter(dfToPlot, YC==yr & HOM_unrd>0), points(MidLon,MidLat,col="red",pch=20))
    
    dev.off()
    
    #length frequency plot
    jpeg(filename=paste0(plot.dir,"/",survey,"_LF_",j,".jpg"), 
         width=800, 
         height=600, 
         quality=100)
    
    dfAllMeas <- lengthData %>% filter(Year==j) %>% group_by(LngtClasscm) %>% summarise(AllatLen = sum(RdNoAtLen)) %>% arrange(LngtClasscm)
    
    dfIGFS.LF <- merge(dfIGFS.LF,dfAllMeas,by.x="Len",by.y="LngtClasscm",all.x=TRUE)
    names(dfIGFS.LF)[names(dfIGFS.LF) == 'AllatLen'] <- paste0("Num_",j)
    
    bar.cols <- c("TRUE"="Red","FALSE"="Blue")
    
    if (nrow(dfAllMeas)>0){
      barplot(dfAllMeas$AllatLen, names.arg = dfAllMeas$LngtClasscm,ylab="", xlab="Length (cm)", 
              axes=F, col=bar.cols[as.character(dfAllMeas$LngtClasscm<=LFCutOff)], cex.names=1.5, cex.lab=1.5)
      text(x=0,y=0.95*max(dfAllMeas$AllatLen),labels=j,cex=1.5,pos=4)
      legend("topright",legend=c("Juvenile","Age 1+"),pch=15, col=c("Red","Blue"),bty="n",cex=1.5)
    }
    
    dev.off()
    
  }
  
  #save(dfIGFS.LF, file=paste0(savedata.dir,"/IGFS_LF.RData"))
  
  
  
  
  
  #total number of HOM
  haulData <- dplyr::left_join(haulData,
                               lengthData %>% group_by(Idx) %>% summarise(HOM=sum(RdNoAtLen)),
                               by="Idx")
  
  haulData[is.na(haulData$HOM),]$HOM <- 0
  
  #total number of HOM (unraised)
  haulData <- dplyr::left_join(haulData,
                               lengthData %>% group_by(Idx) %>% summarise(HOM_unrd=sum(TotNoAtLen)),
                               by="Idx")
  
  haulData[is.na(haulData$HOM_unrd),]$HOM_unrd <- 0
  
  haulData <- dplyr::left_join(haulData, 
                               filter(lengthData,Age0YN==TRUE) %>% group_by(Idx) %>% summarise(Age0=sum(RdNoAtLen)),
                               by="Idx")
  
  haulData[is.na(haulData$Age0),]$Age0 <- 0
  
  #add number of age0 (unraised) to each haul
  haulData <- dplyr::left_join(haulData,
                               dplyr::filter(lengthData,Age0YN) %>% group_by(Idx) %>% summarise(Age0_unrd=sum(TotNoAtLen)),
                               by="Idx")
  
  #replace any NAs with zero
  haulData[is.na(haulData$Age0_unrd),]$Age0_unrd <- 0
  
  #age 1 plus
  haulData <- dplyr::left_join(haulData, 
                               dplyr::filter(lengthData,Age0YN==FALSE) %>% group_by(Idx) %>% summarise(Age1Plus=sum(RdNoAtLen)),
                               by="Idx")
  
  #replace any NAs with zero
  haulData[is.na(haulData$Age1Plus),]$Age1Plus <- 0
  
  #add number of age1 plus (unraised) to each haul
  haulData <- dplyr::left_join(haulData, 
                               dplyr::filter(lengthData,Age0YN==FALSE) %>% group_by(Idx) %>% summarise(Age1Plus_unrd=sum(TotNoAtLen)),
                               by="Idx")
  
  #replace any NAs with zero
  haulData[is.na(haulData$Age1Plus_unrd),]$Age1Plus_unrd <- 0
  
  #fishable biomass (20cm and over)
  haulData <- dplyr::left_join(haulData, 
                               dplyr::filter(lengthData,LngtClasscm>=20) %>% group_by(Idx) %>% summarise(Fishable=sum(RdNoAtLen)),
                               by="Idx")
  
  #replace any NAs with zero
  haulData[is.na(haulData$Fishable),]$Fishable <- 0
  
  #unraised fishable biomass (20cm and over)
  haulData <- dplyr::left_join(haulData,
                               dplyr::filter(lengthData,LngtClasscm>=20) %>% group_by(Idx) %>% summarise(Fishable_unrd=sum(TotNoAtLen)),
                               by="Idx")
  
  #replace any NAs with zero
  haulData[is.na(haulData$Fishable_unrd),]$Fishable_unrd <- 0
  
  #add total catch in kg and total catch (per hour) in kg
  dfTemp <- lengthData %>% group_by(Idx,CatIdentifier,CatCatchWgt) %>% summarise(Age0=sum(RdNoAtLen))
  dfTotCatch <- dfTemp %>% group_by(Idx) %>% summarise(CatchWgt=sum(CatCatchWgt))
  
  haulData <- dplyr::left_join(haulData, dfTotCatch, by="Idx")
  
  #replace any NAs (hauls with no HOM) with zero
  haulData[is.na(haulData$CatchWgt),]$CatchWgt <- 0
  haulData$CatchWgt <- haulData$CatchWgt*0.001
  haulData$RdCatchWgtKg <- haulData$CatchWgt*(60/haulData$HaulDur)
  
  #number at length
  for (l in 1:60) {
    
    haulData <- dplyr::left_join(haulData, 
                                 dplyr::filter(lengthData,LngtClasscm==l) %>% group_by(Idx) %>% summarise(NumAtLen=sum(TotNoAtLen)), 
                                 by="Idx")
    
    haulData[is.na(haulData$NumAtLen),]$NumAtLen <- 0
    
    names(haulData)[which(colnames(haulData)=="NumAtLen")] <- paste0("Num",l,"cm")
    
  }
  
  #create Juv as a boolean
  haulData$Juv <- haulData$Age0>0
  
  #or any size
  haulData$AllHOM <- haulData$HOM>0
  
  #swept area (square m)
  haulData$SweptArea_m2 <- haulData$GroundSpeed*0.5144*haulData$WingSpread*haulData$HaulDur*60
  
  #swept area (square nm) - 1 nm = 1852 m
  haulData$SweptArea_nm2 <- haulData$SweptArea_m2/(1852*1852)
  
  #swept volume
  haulData$SweptVolume_m3 <- haulData$SweptArea_m2*haulData$Netopening
  
  #survey name
  haulData$Survey <- survey
  
  
  # # #age records
  # ageData <- getDATRAS(record="CA",
  #                      survey=survey,
  #                      startyear=startyear,
  #                      endyear=endyear,
  #                      quarters=quarters,
  #                      parallel=TRUE,
  #                      cores=4)
  # 
  # cat(dim(ageData),"\n")
  
  haulData
  
}
