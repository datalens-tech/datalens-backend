#!/usr/bin/env bash

CONFIG_URL="https://raw.githubusercontent.com/datalens-tech/datalens/main/.github/workflows/scripts/changelog/changelog_config.json"

PR_TITLE=${PR_TITLE}
GH_TOKEN=${GH_TOKEN}
REPO=${REPO}
PR_NUMBER=${PR_NUMBER}

CONFIG_DATA=$(curl -s $CONFIG_URL)

TYPE_LABELS=$(echo $CONFIG_DATA | jq '.section_tags.tags[].id' | tr -d '"')
TYPE_LABELS_PREFIX=$(echo $CONFIG_DATA | jq '.section_tags.prefix' | tr -d '"')

COMPONENT_LABELS=$(echo $CONFIG_DATA | jq '.component_tags.tags[].id' | tr -d '"')
COMPONENT_LABELS_PREFIX=$(echo $CONFIG_DATA | jq '.component_tags.prefix' | tr -d '"')

LABELS_TO_ADD=""


for label in $TYPE_LABELS
do
  echo $PR_TITLE | grep -Ei "^$label*" > /dev/null

  if [ $? == 0 ]
  then
    LABELS_TO_ADD+="$TYPE_LABELS_PREFIX$label"
  fi

done

for label in $COMPONENT_LABELS
do
  echo $PR_TITLE | grep -Ei "[a-z]+\(.*$label.*\):*" > /dev/null

  if [ $? == 0 ]
  then
    LABELS_TO_ADD+=",$COMPONENT_LABELS_PREFIX$label"
  fi

done

gh pr edit $PR_NUMBER --repo $REPO --add-label $LABELS_TO_ADD
