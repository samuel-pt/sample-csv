setwd("/home/sam/tmp/csv/sml_opportunity_changes/girish")


changes_data <- read.csv("VFOpportunityChanges5a.csv", stringsAsFactors = FALSE)
act_data <- read.csv("ActivityForOpenOpps_a.csv", stringsAsFactors = FALSE)
changesdf <- merge(changes_data, act_data, by="OpportunityId", all.x=TRUE)

head(changesdf)
colnames(changesdf)[colnames(changesdf)=="LowestAmount"] <- "LowestAmount__c"
colnames(changesdf)[colnames(changesdf)=="HighestAmount"] <- "HighestAmount__c"
colnames(changesdf)[colnames(changesdf)=="NumAmtIncreases"] <- "NumAmtIncreases__c"
colnames(changesdf)[colnames(changesdf)=="NumAmtDecreases"] <- "NumAmtDecreases__c"
colnames(changesdf)[colnames(changesdf)=="AmountChangeActual"] <- "AmountChangeActual__c"
colnames(changesdf)[colnames(changesdf)=="AmountChangePercent"] <- "AmountChangePercent__c"
colnames(changesdf)[colnames(changesdf)=="EarliestCloseDate"] <- "EarliestCloseDate__c"
colnames(changesdf)[colnames(changesdf)=="LatestCloseDate"] <- "LatestCloseDate__c"
colnames(changesdf)[colnames(changesdf)=="NumCloseDtIncreases"] <- "NumCloseDtIncreases__c"
colnames(changesdf)[colnames(changesdf)=="NumCloseDtDecreases"] <- "NumCloseDtDecreases__c"
colnames(changesdf)[colnames(changesdf)=="CloseDateChange"] <- "CloseDateChange__c"
colnames(changesdf)[colnames(changesdf)=="CloseDateQuarterChange"] <- "CloseDateQuarterChange__c"
colnames(changesdf)[colnames(changesdf)=="DaysInCurrentStage"] <- "DaysInCurrentStage__c"
colnames(changesdf)[colnames(changesdf)=="DaysSinceLastActivity"] <- "DaysSinceLastActivity__c"
colnames(changesdf)[colnames(changesdf)=="RepeatedForecastCat"] <- "RepeatedForecastCat__c"
colnames(changesdf)[colnames(changesdf)=="NumTimesFCRepeated"] <- "NumTimesFCRepeated__c"
colnames(changesdf)[colnames(changesdf)=="RepeatedStageName"] <- "RepeatedStageName__c"
colnames(changesdf)[colnames(changesdf)=="NumTimesStageRepeated"] <- "NumTimesStageRepeated__c"
colnames(changesdf)[colnames(changesdf)=="Win_Score"] <- "Win_Score__c"
colnames(changesdf)[colnames(changesdf)=="PositiveReason"] <- "PositiveReason__c"
colnames(changesdf)[colnames(changesdf)=="NegativeReason"] <- "NegativeReason__c"
colnames(changesdf)[colnames(changesdf)=="CloseThisQuarterProbability"] <- "CloseThisQuarterProbability__c"
colnames(changesdf)[colnames(changesdf)=="ActivityCount"] <- "ActivityCount__c"
colnames(changesdf)[colnames(changesdf)=="LatestActivityEmotionDetails"] <- "LatestActivityEmotionDetails__c"
colnames(changesdf)[colnames(changesdf)=="LatestActivityEmotion"] <- "LatestActivityEmotion__c"
colnames(changesdf)[colnames(changesdf)=="ActivityCountDetails"] <- "ActivityCountDetails__c"
# colnames(changesdf)[colnames(changesdf)=="OpportunityId"] <- "Id"
colnames(changesdf)[colnames(changesdf)=="OpportunityId"] <- "Opportunity__c"




.libPaths()
# .libPaths( c( .libPaths(), "/home/garish/R/x86_64-redhat-linux-gnu-library/3.2") )
# .libPaths( c( .libPaths(), "/home/manuel/R/x86_64-redhat-linux-gnu-library/3.2") )
.libPaths( c( .libPaths(), "/home/sam/work/software/apache/spark/spark-1.6.2-bin-hadoop2.6/R/lib") )

library("RForcecom")

sfusername    <- "samspark2@palmtreeinfotech.com"
sfpassword    <- "Passion2016!q7NLg7jI5IvXogJkdKghbat1f"

apiVersion    <- "36.0"
sfversion     <- "36.0"
sfobject      <- "SML_Opportunity_Change__c"
# viewtype and csv_location has to be configured for different objects
viewtype      <- "'Change'"
#csv_location  <- "/newvol/girish/blackduck/forecast_agg_3.csv"

rem_old_rec   <- FALSE





# Initializing Spark
print("Initializing Spark")
Sys.setenv(SPARK_HOME = "/home/sam/work/software/apache/spark/spark-1.6.2-bin-hadoop2.6")
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
Sys.setenv('SPARKR_SUBMIT_ARGS'='"--jars" "/home/sam/work/projects/s/etl/spark-salesforce/target/scala-2.10/spark-salesforce-assembly-1.0.7.jar" "sparkr-shell"')
library(SparkR)
sc <<- sparkR.init(master="local")
sqlContext <<- sparkRSQL.init(sc)




if (rem_old_rec) {
  # Query the old SFDC and constructing it as dataframe
  print("Querying old records to be deleted")
  session       <- rforcecom.login(sfusername, sfpassword, apiVersion=apiVersion)
  soqlQuery <- paste0("Select Id from SML_Opportunity_Change__c where Is_Test_Record__c = true and View_Type__c = ", viewtype)
  #soqlQuery <- paste0("Select Id from SML_Opportunity_Change__c where View_Type__c = ", viewtype)
  result <- rforcecom.query(session, soqlQuery)
  delete_df <- data.frame(result)
  names(delete_df) <- "Id"
  
  if (nrow(delete_df) > 0) {
    # Deleting old records using Salesforce Bulk API
    print("Deleting old records using bulk job")
    job_info <- rforcecom.createBulkJob(session, 
                                        operation='delete', 
                                        object=sfobject)
    batches_info <- rforcecom.createBulkBatch(session, 
                                              jobId=job_info$id, 
                                              data=delete_df)
    # TODO Wait until all the batches are completed
    # But if the data is not huge, it is not needed
    batches_status <- lapply(batches_info, 
                             FUN=function(x){ 
                               rforcecom.checkBatchStatus(session, jobId=x$jobId, batchId=x$id)
                             })
    close_job_info <- rforcecom.closeBulkJob(session, jobId=job_info$id)
  } else {
    print("No records to be deleted")
  }
}






# Contructing R data frame using provided csv file
print("Loading new records in to SFDC")
changesdf$View_Type__c <- "Change"
changesdf$Is_Test_Record__c <- TRUE
r_df <- changesdf
head(r_df)
nrow(r_df)

# Creating Spark Dataframe as Spark Salesforce Connector operates only on Spark Dataframe 
df <- createDataFrame(sqlContext, r_df)

write.df(df, 
         path="", 
         source="com.springml.spark.salesforce", 
         username=sfusername, 
         password=sfpassword, 
         version=sfversion, 
         sfObject=sfobject)



