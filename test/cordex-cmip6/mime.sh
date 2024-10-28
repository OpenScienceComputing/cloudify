#!/bin/bash

CONTAINER_NAME="plots"
NEW_MIME_TYPE="text/html"

# Loop through each object in the container
for object in $(swift list $CONTAINER_NAME); do
    # Change the content type
    swift post "$CONTAINER_NAME" "$object" -H "Content-Type:$NEW_MIME_TYPE"
    echo "Updated MIME type for $object to $NEW_MIME_TYPE"
done
