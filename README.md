# airflow-dag-scheduling
## Data pipeline to extract online stock market data 

This project runs on a docker container. 

### Seting up the container
1. Please, unzip springboard-airflow.zip 
2. Copy market_pipeline.py file to mnt/airflow/dags directory
3. Execute docker-compose ps and you should see status: healthy(running)
4. Go to localhost:8080 - you will be presented with the web UI
5. Use username/password as airflow/airflow


### Dependencies
You will need to install dependencies. 
Please, run on your terminal:
1. pip install airflow
2. pip install yfinance
3. pip install pandas




