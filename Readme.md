Spark Structured Streaming Demo
===============================

A simple demo of Spark Structured Streaming with Kafka and Watermarking.

Files
-----

*   producer.py: Sends fake JSON events to a Kafka topic.
    
*   reset\_topics.py: Clears the Kafka topic for a clean demo.
    
*   default\_trigger.py: Consumes from Kafka (default trigger).
    
*   fixed\_proc\_time\_trigger.py: Consumes from Kafka (5-second trigger).
    
*   continuous\_trigger.py: Consumes from Kafka (continuous trigger).
    
*   watermark.py: A stream-stream join demo (does not use Kafka).
    

Prerequisites
-------------

1.  **Kafka** running at localhost:9092.
    
2.  **Spark** (with spark-submit in your path).
    
3.  pip install kafka-python
    
4.  **Netcat** (nc command for the watermark demo).
    

How to Run: Part 1 (Kafka Triggers)
-----------------------------------

Use 3 separate terminals.

**Terminal 1: Reset Kafka (Run once)**
`   python reset_topics.py   `

**Terminal 2: Start Producer**

`   python producer.py   `

**Terminal 3: Run Spark Job (Choose one)**

_Note: The --packages version (4.0.1) should match your Spark version._

`   # Example for default_trigger.py  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 default_trigger.py   `

_(Use the same command for fixed\_proc\_time\_trigger.py or continuous\_trigger.py)_

How to Run: Part 2 (Watermarking)
---------------------------------

Use 3 separate terminals. This demo does _not_ use Kafka.

**Terminal 1: Impression Stream**

`   nc -lk 9998   `

**Terminal 2: Click Stream**

`   nc -lk 9999   `

**Terminal 3: Run Spark Job**_(Clear the checkpoint first!)_

`   rm -rf ~/spark-checkpoints/demo_4_watermark  spark-submit watermark.py   `

**Send Data:**

1.  **In Terminal 1 (Impressions):** Paste 2025-10-22T18:30:00Z,ad\_123 and hit Enter.
    
2.  **In Terminal 2 (Clicks):** Paste 2025-10-22T18:30:05Z,ad\_123 and hit Enter.
    
3.  **Observe Terminal 3:** A successful join appears.