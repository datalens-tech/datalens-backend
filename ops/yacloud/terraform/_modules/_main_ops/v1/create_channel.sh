# ytr solomon provider can't create solomon channels
# so we have to create it by API

# channel sends all events to juggler

# set variables
# $IAM_TOKEN
# $SOLOMON_ENDPOINT
# $SOLOMON_CHANNEL
# $SOLOMON_PROJECT
# $JUGGLER_ENV_TAG

response=$(curl -s -i -X POST \
      -H 'Content-Type: application/json' \
      -H 'Accept: application/json' \
      -H "Authorization: Bearer $IAM_TOKEN" \
      -d @- \
      $SOLOMON_ENDPOINT/api/v2/projects/$SOLOMON_PROJECT/notificationChannels <<EOF
{
  "id": "$SOLOMON_CHANNEL",
  "projectId": "$SOLOMON_PROJECT",
  "name": "$SOLOMON_CHANNEL",
  "notifyAboutStatuses": [],
  "repeatNotifyDelayMillis": 0,
  "isDefault": false,
  "method": {
    "juggler": {
      "host": "{{annotations.host}}",
      "service": "{{annotations.service}}",
      "instance": "",
      "description": "$JUGGLER_DESCRIPTION",
      "tags": [
        "$JUGGLER_ENV_TAG"
      ]
    }
  }
}
EOF
)

echo $response | head -1 | grep ' 200' || { echo "Can't create solomon channel\n$response" ; exit 1; }
