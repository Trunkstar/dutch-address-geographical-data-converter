#!/bin/bash

# Check if file exists
if [ ! -f "$1" ]; then
    echo "Usage: $0 <file_to_upload>"
    echo "Error: File not found"
    exit 1
fi

FILE_PATH="$1"
FILE_NAME=$(basename "$FILE_PATH")

# Generate a random bin ID
BIN_ID=$(openssl rand -hex 4)

# Upload file
UPLOAD_RESPONSE=$(curl \
    -s \
    --retry 3 \
    --retry-delay 5 \
    --connect-timeout 30 \
    --max-time 3600 \
    -X POST \
    -H "Accept: application/json" \
    -H "cid: github-actions-uploader" \
    -H "Content-Type: application/octet-stream" \
    --data-binary "@$FILE_PATH" \
    "https://filebin.net/$BIN_ID/$FILE_NAME")

if [ $? -ne 0 ]; then
    echo "Failed to upload file"
    echo "Response: $UPLOAD_RESPONSE"
    exit 1
fi

# Extract bin ID and filename from JSON response
RESPONSE_BIN_ID=$(echo "$UPLOAD_RESPONSE" | jq -r '.bin.id')
RESPONSE_FILENAME=$(echo "$UPLOAD_RESPONSE" | jq -r '.file.filename')

if [ -z "$RESPONSE_BIN_ID" ] || [ "$RESPONSE_BIN_ID" = "null" ] || \
   [ -z "$RESPONSE_FILENAME" ] || [ "$RESPONSE_FILENAME" = "null" ]; then
    echo "Failed to extract information from response"
    echo "Response: $UPLOAD_RESPONSE"
    exit 1
fi

# Construct download URL
DOWNLOAD_URL="https://filebin.net/$RESPONSE_BIN_ID/$RESPONSE_FILENAME"

# Verify the upload worked
HTTP_CODE=$(curl -L -s -o /dev/null -w "%{http_code}" "$DOWNLOAD_URL")
if [ "$HTTP_CODE" != "200" ]; then
    echo "Failed to verify upload (HTTP $HTTP_CODE)"
    exit 1
fi

echo "$DOWNLOAD_URL"