#!/bin/bash

#Name of the rule to update
RULE_NAME=acSetNumThreads
#That's what we're going to insert
ISILON_RULE="$RULE_NAME { ON(\$KVPairs.rescType == \"isilon\") { msiSetNumThreads(\"default\",\"1\",\"default\"); } }"

RULE_BASE_DIR=/etc/irods/
RULE_BASE_FILE=core.re
RULE_BASE_PATH=$RULE_BASE_DIR$RULE_BASE_FILE
BAK_FILE_EXT=bak

echo -e "New 'acSetNumThreads' rule will be added to the rule base to force single-stream data transfer for 'isilon' resources by default. The new rule will be added before the first active 'acSetNumThreads' rule\n"

# Check whether 'core.re' exists
if [ ! -f $RULE_BASE_PATH ]; then
    echo -e "Rule base file '$RULE_BASE_PATH' wasn't found. It's OK provided we are on a Resource server, but is an error if we are on an iCAT\n"

    exit 0
fi 

# Check whether rules related to Isilon already exist
grep -qG "^\s*$RULE_NAME\s*{\s*ON\s*(.*\$KVPairs\.rescType\s*==\s*\"isilon\".*$" $RULE_BASE_PATH

if [ $? == 0 ]; then
    echo -e "Some Isilon-related rule 'acSetNumThreads' already exists. Rule base was left unmodified\n"

    exit 0;
fi

# Prepare the name for a backup file
# We use extension ".bak.xxx" for backup files, where "xxx" is an integer number
# To select the name for the next backup file, we first identify already existing backup file
# with the highest "xxx" number and then add 1 to this number

# Conservatively escape each symbol in the file name (so that its symbols are not treated
# as metacharacters in the regular expression below)
RULE_BASE_FILE_ESCAPED=$(echo $RULE_BASE_FILE | sed 's/./\\&/g')
# Find backup file with the highest number
LAST_NUM=$(find $RULE_BASE_DIR -maxdepth 1 -regextype posix-egrep -regex ".*/$RULE_BASE_FILE_ESCAPED\.$BAK_FILE_EXT\.[0-9]{3}$" | sort | tail -1);
LAST_NUM=$(echo ${LAST_NUM##*/$RULE_BASE_FILE.$BAK_FILE_EXT.});

if [ -z "$LAST_NUM" ]; then
    LAST_NUM=0;
else
    LAST_NUM=$(echo $((10#$LAST_NUM+1)));
fi

# Get first active 'acSetNumThreads' rule
ACTIVE_RULE=$(grep -n -m 1 -E "^\s*$RULE_NAME.*$" $RULE_BASE_PATH)

if [ -z "$ACTIVE_RULE" ]; then
    echo -e "We didn't find any active 'acSetNumThreads' rule. We cannot identify an appropriate place for the new rule. Please insert it manually into an appropriate place:\n$ISILON_RULE\n"

    exit 1
fi

# Get line number of the first active rule
LINE_NUM=$(echo $ACTIVE_RULE | cut -d':' -f1)

# Numerical extension for the new backup file
NUM_EXT=$(printf %03d $LAST_NUM)
#Patch rule base file in-place and save backup
sed -i".$BAK_FILE_EXT.$NUM_EXT" "$LINE_NUM i\\$ISILON_RULE" $RULE_BASE_PATH

if [ $? == 0 ]; then 
    echo -e "File '$RULE_BASE_PATH' was successfully patched. Backup of the previous version of the rule base can be found here: $RULE_BASE_PATH.$BAK_FILE_EXT.$NUM_EXT\n"
else
    echo "Error while patching the rule base"

    exit 1
fi
