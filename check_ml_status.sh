#!/bin/bash
# Check ML pipeline status with ETA

cd /Users/johneakin/PyCharmProjects/tx-2026-election-results

echo "=== ML Pipeline Status Check ==="
echo "Time: $(date)"
echo ""

# Check if process is running - get the actual python process, not the shell
# Look for processes running the ML pipeline scripts (old and new locations)
PROCESS_INFO=$(ps aux | grep -E "python.*run_new_steps|python.*run_ml_steps|python.*src/scripts/run|uv run.*run_ml|uv run.*run_new" | grep -v grep | head -1)
if [ -z "$PROCESS_INFO" ]; then
    # Try alternative patterns - look for script names in command line
    PROCESS_INFO=$(ps aux | grep -E "run_new_steps\.py|run_ml_steps\.py" | grep -v grep | head -1)
fi
if [ -z "$PROCESS_INFO" ]; then
    # Try looking for processes with high CPU that might be the ML pipeline
    # (ML processes typically use high CPU during feature engineering/training)
    PROCESS_INFO=$(ps aux | awk '$3 > 50 && /python/ && /run/ {print}' | grep -v grep | head -1)
fi

if [ -n "$PROCESS_INFO" ]; then
    echo "✓ Process is RUNNING"
    PID=$(echo "$PROCESS_INFO" | awk '{print $2}')
    CPU=$(echo "$PROCESS_INFO" | awk '{print $3}')
    MEM=$(echo "$PROCESS_INFO" | awk '{print $4}')
    TIME_STR=$(echo "$PROCESS_INFO" | awk '{print $10}')
    
    # Extract full command for verification
    COMMAND=$(echo "$PROCESS_INFO" | awk '{for(i=11;i<=NF;i++) printf "%s ", $i; print ""}' | sed 's/ $//')
    
    echo "  PID: $PID"
    echo "  CPU: ${CPU}%"
    echo "  MEM: ${MEM}%"
    echo "  Runtime: $TIME_STR"
    echo "  Command: $COMMAND"
    
    # Parse TIME_STR (format: MM:SS.SS or HH:MM:SS.SS)
    ELAPSED_SECONDS=0
    # Remove decimal part if present
    TIME_CLEAN=$(echo "$TIME_STR" | cut -d'.' -f1)
    if [[ "$TIME_CLEAN" =~ ^([0-9]+):([0-9]+):([0-9]+)$ ]]; then
        # HH:MM:SS format
        HOURS=${BASH_REMATCH[1]}
        MINUTES=${BASH_REMATCH[2]}
        SECONDS=${BASH_REMATCH[3]}
        ELAPSED_SECONDS=$((HOURS * 3600 + MINUTES * 60 + SECONDS))
    elif [[ "$TIME_CLEAN" =~ ^([0-9]+):([0-9]+)$ ]]; then
        # MM:SS format
        MINUTES=${BASH_REMATCH[1]}
        SECONDS=${BASH_REMATCH[2]}
        ELAPSED_SECONDS=$((MINUTES * 60 + SECONDS))
    fi
    
    ELAPSED_MINUTES=$((ELAPSED_SECONDS / 60))
    
    # Check for output files to determine progress
    STEP="Unknown"
    ETA_MINUTES="Unknown"
    
    if [ -f "data/exports/models/party_prediction_model.pkl" ]; then
        STEP="Step 14-15 (Predictions & Analysis)"
        # Model exists, likely in prediction/analysis phase
        # Typical prediction phase: 10-20 minutes for 18M voters
        if [ $ELAPSED_MINUTES -lt 5 ]; then
            ETA_MINUTES="15-25"
        else
            ETA_MINUTES="10-20"
        fi
    elif [ -f "data/exports/parquet/early_voting_merged.parquet" ] && [ ! -f "data/exports/models/party_prediction_model.pkl" ]; then
        STEP="Step 13 (Model Training)"
        # Feature engineering typically takes 10-30 minutes, training takes 5-15 minutes
        if [ $ELAPSED_MINUTES -lt 15 ]; then
            ETA_MINUTES=$((30 - ELAPSED_MINUTES))
        elif [ $ELAPSED_MINUTES -lt 30 ]; then
            ETA_MINUTES=$((45 - ELAPSED_MINUTES))
        else
            ETA_MINUTES="10-20"
        fi
    else
        STEP="Step 12-13 (Feature Engineering/Model Training)"
        # Feature engineering: 10-30 minutes, Training: 5-15 minutes
        # Total estimated: 40-60 minutes
        if [ $ELAPSED_MINUTES -lt 10 ]; then
            ETA_MINUTES=$((50 - ELAPSED_MINUTES))
        elif [ $ELAPSED_MINUTES -lt 20 ]; then
            ETA_MINUTES=$((60 - ELAPSED_MINUTES))
        elif [ $ELAPSED_MINUTES -lt 30 ]; then
            ETA_MINUTES=$((70 - ELAPSED_MINUTES))
        elif [ $ELAPSED_MINUTES -lt 45 ]; then
            ETA_MINUTES=$((80 - ELAPSED_MINUTES))
        else
            ETA_MINUTES="10-30"
        fi
    fi
    
    echo "  Current Step: $STEP"
    echo "  Elapsed Time: ${ELAPSED_MINUTES} minutes"
    echo "  Estimated Time Remaining: ~${ETA_MINUTES} minutes"
    
    # Calculate estimated completion time (handle both macOS and Linux date commands)
    if [[ "$ETA_MINUTES" =~ ^[0-9]+$ ]]; then
        if command -v gdate > /dev/null; then
            COMPLETION_TIME=$(gdate -d "+${ETA_MINUTES} minutes" +"%H:%M:%S" 2>/dev/null)
        elif [[ "$OSTYPE" == "darwin"* ]]; then
            COMPLETION_TIME=$(date -v+${ETA_MINUTES}M +"%H:%M:%S" 2>/dev/null)
        else
            COMPLETION_TIME=$(date -d "+${ETA_MINUTES} minutes" +"%H:%M:%S" 2>/dev/null)
        fi
        if [ -n "$COMPLETION_TIME" ]; then
            echo "  Estimated Completion: ~$COMPLETION_TIME"
        fi
    fi
else
    echo "✗ Process is NOT running (may have completed or failed)"
fi

echo ""

# Check for output files
echo "=== Output Files ==="
if [ -f "data/exports/models/party_prediction_model.pkl" ]; then
    MODEL_SIZE=$(ls -lh data/exports/models/party_prediction_model.pkl 2>/dev/null | awk '{print $5}')
    if [[ "$OSTYPE" == "darwin"* ]]; then
        MODEL_TIME=$(stat -f "%Sm" -t "%H:%M:%S" data/exports/models/party_prediction_model.pkl 2>/dev/null)
    else
        MODEL_TIME=$(stat -c "%y" data/exports/models/party_prediction_model.pkl 2>/dev/null | cut -d' ' -f2 | cut -d'.' -f1)
    fi
    echo "✓ Model file exists: ${MODEL_SIZE} (created: ${MODEL_TIME})"
else
    echo "✗ Model file not created yet"
fi

if [ -f "data/exports/parquet/voters_with_party_modeling.parquet" ]; then
    DATA_SIZE=$(ls -lh data/exports/parquet/voters_with_party_modeling.parquet 2>/dev/null | awk '{print $5}')
    if [[ "$OSTYPE" == "darwin"* ]]; then
        DATA_TIME=$(stat -f "%Sm" -t "%H:%M:%S" data/exports/parquet/voters_with_party_modeling.parquet 2>/dev/null)
    else
        DATA_TIME=$(stat -c "%y" data/exports/parquet/voters_with_party_modeling.parquet 2>/dev/null | cut -d' ' -f2 | cut -d'.' -f1)
    fi
    echo "✓ Modeled data file exists: ${DATA_SIZE} (created: ${DATA_TIME})"
else
    echo "✗ Modeled data file not created yet"
fi

echo ""

# Check log file
if [ -f "ml_pipeline_run.log" ] && [ -s "ml_pipeline_run.log" ]; then
    LOG_LINES=$(wc -l < ml_pipeline_run.log | tr -d ' ')
    echo "=== Recent Log Output (last 10 lines, total: $LOG_LINES lines) ==="
    tail -10 ml_pipeline_run.log
else
    echo "Log file is empty or doesn't exist"
fi
