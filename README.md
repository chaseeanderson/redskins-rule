# The "Redskins Rule"

This is an ETL project aimed at testing the predictive "Redskins Rule", which attempts to predict the outcome of a presidential election based on the results of the last Washington Commanders (formerly known as the Washington Redskins) home game prior to the presidential election. 

The original rule states: 

> If the Redskins win their last home game before the election, the party that won the previous election wins the next election and that if the Redskins lose, the challenging party's candidate wins.

With an amendment stating: 

> When the popular vote winner does not win the election, the impact of the Redskins game on the subsequent presidential election gets flipped.

## Project Technologies

_TODO - insert technologies flowchart_

- Docker
- Airflow
- PySpark
- Google Cloud (Storage, BigQuery, Looker)
- Python libraries of note: Pandas, BeautifulSoup

## Methodology

All technologies are run via Docker containers and all stages are orchestrated via Airflow. 

### Ingest

- NFL data is ingested via ESPN's open [Team Schedules api](https://gist.github.com/nntrn/ee26cb2a0716de0947a0a4e9a157bc1c#teams:~:text=site.api.espn.com/apis/site/v2/sports/football/nfl/teams/%7BTEAM_ID%7D/schedule%3Fseason%3D%7BYEAR%7D). Airflow jobs are run annually, ingesting the Washington Commander's schedule (with game outcomes) for each year.
- Election data is ingested once via a custom python webscraper built to harvest election results from Britannica's [United States Presidential Election Results](https://www.britannica.com/topic/United-States-Presidential-Election-Results-1788863).
- Source data is pre-processed and stored in GCS buckets. 

### Transform

- Data is transformed using PySpark, reading from the raw data uploaded to GCS. NFL and election data are combined and reduced, creating single rows that show the results of each election in combination with the results of the last Washington Commanders home game prior to the election date. 
- Prediction logic is added and tested against the NFL / election results.  

### Load

- Transformed, harmonized data is uploaded to GCS. 
- BigQuery table is built using the transformed data. 

### Visualize

- Final results are visualized in Looker, using the BQ table as source data. 

## Results

- Final Looker visualizations can be found [here!](https://lookerstudio.google.com/reporting/74ed28eb-0400-4864-b713-aa0cf05a8e47)

## Data Limitations

ESPN's NFL Team Schedules API does not provide pre-late November game scores for years before 2000, limiting the final output. 

## Disclaimer

The term "Redskins" is used in this document only in reference to the "Redskins Rule", a historical term for a predictive rule related to U.S. presidential elections. I acknowledge and understand that the term is offensive and has been rightfully discontinued in reference to the Washington football franchise, which is now known as the Washington Commanders. The use of the term in this document is not intended to be disrespectful or offensive, but is maintained solely for historical accuracy and to preserve reference to the rule.