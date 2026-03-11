#!/bin/bash

# Check if target size was provided
if [ -z "$1" ]; then
    echo "Usage: $0 <target_size_in_GB>"
    echo "Example: $0 50"
    exit 1
fi

# Configuration
DATA_DIR="./data"
SOURCE_FILE="$DATA_DIR/0000.parquet"
TARGET_SIZE=$1
INCREMENT_SIZE="2.6"

# 1. Check if source file exists
if [ ! -f "$SOURCE_FILE" ]; then
    echo "Error: Source file $SOURCE_FILE not found."
    exit 1
fi

# 2. Calculate Current Folder Size in GB
# 'du -sk' gets size in KB, then we convert to GB for the math
CURRENT_SIZE_KB=$(du -sk "$DATA_DIR" | cut -f1)
CURRENT_SIZE_GB=$(echo "scale=2; $CURRENT_SIZE_KB / 1024 / 1024" | bc -l)

echo "Current folder size: ${CURRENT_SIZE_GB} GB"
echo "Target folder size:  ${TARGET_SIZE} GB"

# 3. Validate Target Size (Must be > Current Size)
if (( $(echo "$TARGET_SIZE <= $CURRENT_SIZE_GB" | bc -l) )); then
    echo "Error: Target size must be greater than current folder size (${CURRENT_SIZE_GB} GB)."
    exit 1
fi

# 4. Calculate Number of Copies Needed
# Logic: ceil((target - current) / 2.6)
RAW_DIFF=$(echo "$TARGET_SIZE - $CURRENT_SIZE_GB" | bc -l)
NUM_COPIES=$(echo "scale=10; $RAW_DIFF / $INCREMENT_SIZE" | bc -l | awk '{print int($1 == int($1) ? $1 : $1 + 1)}')

# Find the next available index so we don't overwrite existing files
# It looks for the highest 4-digit number in the folder and starts from there + 1
LAST_INDEX=$(ls "$DATA_DIR" | grep -E '^[0-9]{4}\.parquet$' | sort | tail -n 1 | cut -f1 -d'.')
START_INDEX=$((10#$LAST_INDEX + 1))

echo "Need to add approximately $RAW_DIFF GB. Creating $NUM_COPIES copies..."

# 5. Loop to create copies
for (( i=0; i<NUM_COPIES; i++ ))
do
    CURRENT_IDX=$((START_INDEX + i))
    NEW_FILENAME=$(printf "$DATA_DIR/%04d.parquet" $CURRENT_IDX)
    
    cp "$SOURCE_FILE" "$NEW_FILENAME"
    echo "Created: $NEW_FILENAME"
done

echo "Done! Final estimated size: $(echo "$CURRENT_SIZE_GB + ($NUM_COPIES * $INCREMENT_SIZE)" | bc -l) GB"