#!/bin/bash

# Exit if any of the intermediate steps fail
set -e

# Extract "fqdn" argument from the input into
# FQDN shell variable.
# jq will ensure that the values are properly quoted
# and escaped for consumption by the shell.
eval "$(jq -r '@sh "FQDN=\(.fqdn)"')"

#Get the host address by its fqdn
HOST=$(dig +short "$FQDN")

# Safely produce a JSON object containing the result value.
# jq will ensure that the value is properly quoted
# and escaped to produce a valid JSON string.
jq -n --arg host "$HOST" '{"host":$host}'