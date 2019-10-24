# Creditas Data Engineering Challenge

## Instructions

To reproduce the solution you just have to:
    - Navigate to the project root directory.
    - Run "docker build -t creditas_challenge ." to build a docker image for the project.
    - Run "docker run creditas_challenge" to run the docker image.

## Answers

### 1. What was the most expensive campaign?
```sql
SELECT campaign_id, campaign_name, origin, sum(cost) AS total_campaign_cost  FROM 
(
    SELECT cost, google_campaign_id AS campaign_id, google_campaign_name AS campaign_name, origin from google_ads 
    UNION ALL 
    SELECT cost, facebook_campaign_id, facebook_campaign_name, origin from facebook_ads
) 
GROUP BY campaign_id, origin
ORDER BY total_campaign_cost DESC
LIMIT 1
```

### 2. What was the most profitable campaign?
```sql
SELECT campaigns.campaign_id, campaigns.campaign_name, campaigns.origin, total_revenue, total_campaign_cost, total_revenue - total_campaign_cost AS total_profit
FROM 
(
    SELECT campaign_id, campaign_name, origin, sum(cost) AS total_campaign_cost  FROM 
    (
        SELECT cost, google_campaign_id AS campaign_id, google_campaign_name AS campaign_name, origin from google_ads 
        UNION ALL 
        SELECT cost, facebook_campaign_id, facebook_campaign_name, origin from facebook_ads
    ) 
    GROUP BY campaign_id, origin
    ORDER BY total_campaign_cost DESC
) 
AS campaigns, 
(
    SELECT page_view.campaign_id, SUM(customer_leads_funnel.revenue) AS total_revenue
    FROM page_view, customer_leads_funnel
    WHERE page_view.device_id = customer_leads_funnel.device_id 
    AND page_view.campaign_id IS NOT NULL 
    AND revenue NOT NULL
    GROUP BY page_view.campaign_id, page_view.referer
    ORDER BY  SUM(customer_leads_funnel.revenue) DESC
) 
AS most_profitable_campaign
WHERE campaigns.campaign_id = most_profitable_campaign.campaign_id
ORDER BY total_profit DESC
LIMIT 1
```

### 3. Which ad creative is the most effective in terms of clicks?
```sql
SELECT ad_creative_id, ad_creative_name, SUM(clicks) AS total_clicks
FROM google_ads
GROUP BY ad_creative_id
ORDER BY total_clicks DESC    
LIMIT 1
```

### 4. Which ad creative is the most effective in terms of generating leads?
```sql
SELECT ad_creatives.ad_creative_id, ad_creatives.ad_creative_name,leads  FROM 
(
    SELECT ad_creative_id, ad_creative_name
    FROM google_ads
    GROUP BY ad_creative_id
) AS ad_creatives,
(
    SELECT customer_leads_funnel.device_id, page_view.ad_creative_id, COUNT(customer_leads_funnel.device_id) AS leads
    FROM customer_leads_funnel, page_view
    WHERE page_view.device_id = customer_leads_funnel.device_id
    AND page_view.ad_creative_id IS NOT NULL
    GROUP BY page_view.ad_creative_id
    ORDER BY leads DESC
    LIMIT 1
) AS most_effective_ad_creative
WHERE ad_creatives.ad_creative_id = most_effective_ad_creative.ad_creative_id

```

## Extra questions

### - What would you suggest to process new incoming files several times a day?
#### I would suggest, to process new incoming files several times a day, to use a nice ETL tool for data Extraction, Transformation and Loading, saving time by automating the process.

### - What would you suggest to process new incoming data in near real time?
#### To accomplish that I would suggest to use a stream-processing software like Kafka that is a really good tool for real-time data feeds.

### - What would you suggest to process data that is much bigger?
#### To process data that is much bigger I would recommend to use the pyspark library that is more appropriate to this scenario.

### - What would you suggest to process data much faster?
#### I would suggest a fast ETL tool like AWS Glue, and also a fast database like Elasticsearch
