# Sparkify Data Warehouse

This project offers the ETL pipeline and data warehouse design as the solution for Sparkify to 
further improve its analytical capabilities, by utilizing cloud technology, namely (AWS).
Solution comprises, loading JSON data from an S3 bucket, into staging tables in Redshift, and then creating a dimensional design from the staging data with song streaming events as the fact metric.
## Data Model
![data model.png](resources%2Fdata%20model.png)

---
## How to Operate:
### Prerequisites:
1. An IAM role with permissions to "AmazonS3ReadOnlyAccess" and with Redshift as a trusted entity.
2. A Redshift cluster (needs to be publicly accessible)
   * You can find a function under the "utils.py" module to create the cluster for you, but it will require an IAM user credentials to start up and client and request provisioning of the resources.
   * utils.py also includes a function to delete the cluster (To avoid extra costs).
3. Placeholders in "dwh.cfg" are replaced with appropriate values.

### Environment Setup
1. Create virtual environment and activate it
    ```
    python3.10 -m vevv .venv
    source .venv/bin/activate
    ```

2. Install required libs
    ```
    pip install -r requirements.txt
    ```

### Creating Data Model
1. Replace the placeholders existing **"dwh.cfg"** with the appropriate values.
2. Run file **"create_tables.py"** \
   Terminal command:
   ```
   python create_table.py
   ```
### Load Data from S3 into the Analytics Tables
1. Run file **"create_tables.py"** \
   Terminal command:
   ```
   python etl.py
   ```
---
## Example Queries
The following are example analytical queries that can be performed on the data. \
_(Queries could be executed through "Query Editor" in Redshift)_  


#### Query 1 - How many songs are played at every hour of the day?
```
SELECT t.hour, COUNT(sp.songplay_id) num_songs_played
FROM songplay sp JOIN "time" t ON sp.start_time = t.start_time
GROUP BY t.hour
ORDER BY t.hour
```

#### Query 2 - At the hour with most song plays, what was the subscription type of the users?
```
SELECT sp.level, COUNT(sp.songplay_id) num_songs_played
FROM songplay sp JOIN "time" t ON sp.start_time = t.start_time 
WHERE t.hour = 16
GROUP BY sp.level
```

#### Query 3 - Who are the top 10 users with the most songs played?
```
SELECT u.user_id, u.first_name, u.last_name, COUNT(sp.songplay_id) num_songs_played
FROM songplay sp JOIN "user" u ON sp.user_id = u.user_id
GROUP BY u.user_id, u.first_name, u.last_name
ORDER BY num_songs_played DESC
LIMIT 10
```

