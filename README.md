# crypto-md-capture
Realtime crypto recording application.

Subscribes to realtime order book update messages from the Luno exchange over Websocket. Builds order book state and computes trades from order fill updates. Sends trades to GCP Pub/Sub topic for Dataflow processing into BigQuery (Dataflow jobs managed outside this codebase)
