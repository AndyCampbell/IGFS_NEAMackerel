#driver script
rm(list=ls())
gc()


#library(devtools)
#for functions to download from DATRAS
#install.packages("FLCore",repos="http://flr-project.org/R")
#devtools::install_github("ICES-dk/rICES")
#library(rICES)

library(XML, quietly = TRUE)
library(doParallel, quietly = TRUE)
library(parallel, quietly = TRUE)
library(foreach, quietly = TRUE)
library(data.table, quietly = TRUE)
library(stringr)
library(dplyr)
library(lubridate)

source(".\\Source\\Functions.R")

rawdata.dir = file.path(getwd(),"RawData")
savedata.dir = file.path(getwd(),"RData")

#read in the data (or read from previous save if refresh = FALSE)

dfMac <- fRefreshData(rawdata.dir, savedata.dir,
                      surveys = c("IE-IGFS"),
                      startyear=2009, endyear=2009,
                      refresh = TRUE)
