#!/bin/bash

#Read JOBID File and get the output of the compliancy check
#Save the output in a log file
#This log file will be retrieved by Patching Tools server (fr0-vsiaas-6289)

#CHANGELOG
# Jn Richard - 26-04-2024 - Changed ${LINE} (=hostname.domain)to hostname without domain to get also compliancy status of systems where local domain differs from DNS/CMDB/Sat6


USER=$(whoami)
DATE=$(date '+%d-%m-%Y %H:%M:%S')
DATEAUX=$(date '+%Y%m%d')
#DATA_FOLDER="/local/home/${USER}/patching-data"
DATA_FOLDER="/local/opt/patching_opt_tools/patching-data"
JOBS_ID_LOG="${DATA_FOLDER}/LOGS/jobs_ID.log"
JOBS_HISTORY="${DATA_FOLDER}/LOGS/jobs-history/${DATEAUX}_jobs_ID.log"
COMPLIANT_OUTPUT_LOG="${DATA_FOLDER}/LOGS/sat6_compliant_output.log"
COMPLIANT_OUTPUT_HISTORY="${DATA_FOLDER}/LOGS/compliancy-history/${DATEAUX}_sat6_compliant_output.log"
TEMPLOG="/tmp/cron_get_compliancy_output.log"

#Copy JOBS ID file to have historical data for every day
#After every campaign, all files created should be compressed and then deleted.
cp $JOBS_ID_LOG $JOBS_HISTORY


#Clean file
> $COMPLIANT_OUTPUT_LOG

while read JOBID
    do
        for LINE in `hammer job-invocation info --id ${JOBID} | grep -E "^- Name:" | awk '{print $3}'`
        do
                echo "SERVER: ${LINE}"
               # grep $LINE (hostname.domain) changed to hostname without domain to get also compliancy status of systems where local domain differs from DNS/CMDB/Sat6
               #hammer job-invocation output --id ${JOBID} --async --host "${LINE}" | grep ${LINE} >> $COMPLIANT_OUTPUT_LOG
                hammer job-invocation output --id ${JOBID} --async --host "${LINE}" | grep `echo ${LINE}|cut -f1 -d"."` >> $COMPLIANT_OUTPUT_LOG
        done
        #for LINE in `hammer job-invocation info --id ${JOBID} | grep -E "^ - " | awk '{print $2}'`; do echo "SERVER: ${LINE}"; hammer job-invocation output --id ${JOBID} --host "${LINE}"; done
    done < $JOBS_ID_LOG

#Copy Compliancy output file to have historical data for every day
#After every campaign, all files created should be compressed and then deleted.
cp $COMPLIANT_OUTPUT_LOG $COMPLIANT_OUTPUT_HISTORY

#Send output file to patching tools server
scp $COMPLIANT_OUTPUT_LOG patchtool@fr0-vsiaas-6829:sat6_data/sat6_compliant_output_auto.raw

#for LINE in `hammer job-invocation info --id ${JOBID} | grep -E "^ - " | awk '{print $2}'`; do echo "SERVER: ${LINE}"; hammer job-invocation output --id ${JOBID} --host "${LINE}" | grep ${LINE}; done
#for LINE in `hammer job-invocation info --id ${JOBID} | grep -E "^ - " | awk '{print $2}'`; do echo "SERVER: ${LINE}"; hammer job-invocation output --id ${JOBID} --host "${LINE}"; done

echo "Process to retrieve the compliance output information finished at: ${DATE}" >> $TEMPLOG
cron_get_compliancy_output.sh
Displaying cron_get_compliancy_output.sh.