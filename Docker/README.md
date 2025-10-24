# Google Analytics Data Pipeline Container
  
This Docker container runs your Python ETL pipeline for Google Analytics data, extracting from APIs, validating, and loading into BigQuery.
  
## Build the Container
  
```bash
docker build -t ga-etl-pipeline .

docker run --rm \
  -v $HOME/.gcp/your-service-account.json:/app/key.json \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/key.json \
  ga-etl-pipeline

  ## Required Environment Variables  
  
| Variable     | Description                       | Example Value                              |  
|--------------|-----------------------------------|--------------------------------------------|  
| PROJECT_ID   | Google Cloud project ID           | ldshfgdshuey-1763465                       |  
| BUCKET_NAME  | GCS bucket name                   | dish_data_extraction                       |  
| API_KEY      | API authentication key            | """"                                       |  
| HEADERS      | API request headers (JSON string) | '{"X-API-Key": ""}'                        |  
| BASE_URL     | API base URL                      |                                            |  
| ENDPOINTS    | API endpoints (JSON string)       | '{"daily_visits":"/daily-visits","ga_sessions":"/ga-sessions-data"}' |  
| DATASET      | BigQuery dataset name             | dish_dataset                               |  
  
## Example Run  
  
```bash  
docker run --rm \  
  -e PROJECT_ID=your-gcp-project \  
  -e BUCKET_NAME=your-bucket \  
  -e API_KEY=your-api-key \  
  -e HEADERS='{"X-API-Key":"your-api-key"}' \  
  -e BASE_URL=https://your.api.url \  
  -e ENDPOINTS='{"daily_visits":"/daily-visits","ga_sessions":"/ga-sessions-data"}' \  
  -e DATASET=your_dataset \  
  -v $HOME/.gcp/key.json:/app/key.json \  
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/key.json \  
  ga-etl-pipeline --run_type test  