# crypto-md-capture
Realtime crypto market data recording application.

Subscribes to realtime order book update messages from the Luno exchange over websocket. 
Builds order book state from initials, and then computes trades from order fill updates (deltas). 
Sends trades to GCP Pub/Sub topic for Dataflow processing into BigQuery (Dataflow job uses the  
Google supplied PubSub_Subscription_to_BigQuery DataFlow template and is setup and managed outside this codebase
from the GCP CLI):
eg:

```
gcloud dataflow jobs run dataflow_pubsub_to_bigquery 
    --gcs-location=gs://dataflow-templates/latest/PubSub_Subscription_to_BigQuery 
    --region europe-west2 
    --staging-location gs://dataflow_temp_storage-streamingcrypto/temp 
    --parameters 
        inputSubscription=projects/692233547485/subscriptions/luno_big_query_subscription,
        outputTableSpec=streamingcrypto:streaming_crypto.s_prices_crypto_intraday
```