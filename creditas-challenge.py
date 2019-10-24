#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from urllib import parse
import sqlite3

con = sqlite3.connect("database/db.sqlite")


# In[2]:


def get_info_dict(linha):
    url, device_id, referer = linha.split('|')
    url, device_id, referer = url.split('://')[1].strip(), device_id.split(':')[1].strip(), ''.join(referer.split(':')[1:]).strip()
    referer = 'facebook' if 'facebook' in referer else 'google' if 'google' in referer else referer
    
    params_dict = parse.parse_qs(parse.urlparse(url).query)
    for key in params_dict.keys():
        params_dict[key] = params_dict[key][0]

    info_dict = {
        'url':url,
        'device_id':device_id,
        'referer':referer
    }
    return {**info_dict, **params_dict}

def parse_page_view_to_df():
    info_dict = []
    with open(dataset_folder+page_view_path,'r') as page_file:
        for linha in page_file:        
            info_dict.append(get_info_dict(linha))
    return pd.DataFrame(info_dict)


# In[3]:


dataset_folder = 'datasets/'
google_path = 'google_ads_media_costs.jsonl'
facebook_path = 'facebook_ads_media_costs.jsonl'
page_view_path = 'pageview.txt'

col_names = ['device_id', 'lead_id', 'registered_at', 'credit_decision', 'credit_decision_at', 'signed_at', 'revenue']
customer_funnel_path = 'customer_leads_funnel.csv'


# In[6]:


google_df = pd.read_json(dataset_folder+google_path, lines=True)
facebook_df = pd.read_json(dataset_folder+facebook_path, lines=True)
customer_funnel_df = pd.read_csv(dataset_folder+customer_funnel_path, names=col_names)
page_view_df = parse_page_view_to_df()


# In[6]:


google_df['origin'] = 'google'
facebook_df['origin'] = 'facebook'


# In[7]:


google_df.to_sql('google_ads',con, if_exists='replace')
facebook_df.to_sql('facebook_ads',con, if_exists='replace')
customer_funnel_df.to_sql('customer_leads_funnel',con, if_exists='replace')
page_view_df.to_sql('page_view',con, if_exists='replace')


# In[70]:


print('\n---\n')
df = pd.read_sql_query('''
    SELECT campaign_id, campaign_name, origin, sum(cost) AS total_campaign_cost  FROM 
    (
        SELECT cost, google_campaign_id AS campaign_id, google_campaign_name AS campaign_name, origin from google_ads 
        UNION ALL 
        SELECT cost, facebook_campaign_id, facebook_campaign_name, origin from facebook_ads
    ) 
    GROUP BY campaign_id, origin
    ORDER BY total_campaign_cost DESC
    LIMIT 1
''', con)

print('Most expensive campaign: ',df.campaign_name[0],'\nID: ',df.campaign_id[0],'\nCost: ',df.total_campaign_cost[0])


# In[75]:


print('\n---\n')
df = pd.read_sql_query('''
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
''', con)
#df
print('Most profitable campaign: ',df.campaign_name[0],'\nID: ',df.campaign_id[0],'\nProfit: ',df.total_profit[0])


# In[139]:


print('\n---\n')
df = pd.read_sql_query('''
    SELECT ad_creative_id, ad_creative_name, SUM(clicks) AS total_clicks
    FROM google_ads
    GROUP BY ad_creative_id
    ORDER BY total_clicks DESC    
    LIMIT 1
''', con)

print('Ad creative most effective on clicks: ',df.ad_creative_name[0],'\nID: ',df.ad_creative_id[0],'\nClicks: ',df.total_clicks[0])


# In[143]:


print('\n---\n')
df = pd.read_sql_query('''
    SELECT ad_creatives.ad_creative_id, ad_creatives.ad_creative_name,leads  FROM (
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
    
''', con)

print('Ad creative most effective on leads: ',df.ad_creative_name[0],'\nID: ',df.ad_creative_id[0],'\nLeads: ',df.leads[0])
print('\n---\n')

# In[144]:


con.close()


# In[ ]:




