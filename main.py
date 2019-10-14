#!/usr/bin/env python
# coding: utf-8

# # Long-term international migration 2.04, main reason for migration

# In[195]:


from gssutils import *
from databaker.framework import *

def left(s, amount):
    return s[:amount]

def right(s, amount):
    return s[-amount:]

def mid(s, offset, amount):
    return s[offset:offset+amount]

scraper = Scraper('https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/'                   'internationalmigration/datasets/'                   'longterminternationalmigrationmainreasonformigrationtable204')
scraper


# In[196]:


tabs = scraper.distributions[0].as_databaker()

for i in tabs:
    print(i.name)


# In[197]:


tidied_sheets = []

for tab in tabs:
    if not tab.name.startswith('Table 2.04'):
        continue

    year = tab.filter("Year").expand(DOWN).regex(r'[0-9]{4}(\.0)?').is_not_blank()
    obs = tab.filter("Year").fill(RIGHT).is_not_blank() | tab.filter("Year").shift(DOWN).fill(RIGHT).is_not_blank()
    flow = tab.filter("Year").expand(DOWN).one_of(['Inflow', 'Outflow'])
    reason = tab.filter("Year").expand(RIGHT).is_not_blank()
    reason2 = tab.filter("Year").shift(DOWN).fill(RIGHT).is_not_blank()
    geography = tab.filter("Year").expand(DOWN).one_of(['United Kingdom', 'England and Wales'])
    
    observations = year.fill(RIGHT) & obs.fill(DOWN) 
    observations_ci = observations.shift(RIGHT)
    
    dimensions = [
            HDim(year, 'Year', CLOSEST, ABOVE),
            HDim(flow, 'Migration Flow', CLOSEST, ABOVE),
            HDim(geography, 'Geography', CLOSEST, ABOVE),
            HDim(reason, 'Reason for Migration', CLOSEST, LEFT),
            HDimConst('Unit','People (thousands)'),
            HDim(observations_ci, 'CI', DIRECTLY, RIGHT),
            HDimConst('Measure Type', 'Count'),
            HDim(reason2, 'Reason2', CLOSEST, LEFT)
    ]
    
    tidy_sheet = ConversionSegment(tab, dimensions, observations)
    savepreviewhtml(tidy_sheet, fname="Preview.html")
    
    tidied_sheets.append(tidy_sheet.topandas())
    
import pandas as pd

df = pd.concat(tidied_sheets, ignore_index = True).fillna('')
df['Year'] = df.apply(lambda x: int(float(x['Year'])), axis = 1)
df['Reason for Migration'] = df.apply(lambda x: x['Reason for Migration'][:-1] if x['Reason for Migration'].endswith('2') else x['Reason for Migration'], axis = 1)
df['Reason for Migration'] = df.apply(lambda x: x['Reason for Migration'] if x['Reason2'] == '' else x['Reason for Migration'] + ' - ' + x['Reason2'], axis = 1)
df['Reason for Migration'] = df.apply(lambda x: x['Reason for Migration'][:-1] if x['Reason for Migration'].endswith('1') else x['Reason for Migration'], axis = 1)
df['CI'] = df.apply(lambda x: left(x['CI'], len(str(x['CI'])) - 2) if x['CI'].endswith('.0') else x['CI'], axis = 1)
df = df.drop(['Reason2'], axis = 1)
df.rename(columns={'OBS':'Value',
                   'DATAMARKER':'Marker'}, 
                   inplace=True)
df


# In[198]:


tidy = df[['Geography', 'Year', 'Reason for Migration', 'Migration Flow',
             'Measure Type','Value','CI','Unit','Marker']]
tidy['Marker'] = tidy.apply(lambda x: 'not-available' if x['Marker'] == ':' else x['Marker'], axis = 1)

from IPython.core.display import HTML
for col in tidy:
    if col not in ['Value', 'CI']:
        tidy[col] = tidy[col].astype('category')
        display(HTML(f"<h2>{col}</h2>"))
        display(tidy[col].cat.categories)


# In[199]:


tidy['Geography'] = tidy['Geography'].cat.rename_categories({
    'United Kingdom': 'K02000001',
    'England and Wales': 'K04000001'
})
tidy['Migration Flow'].cat.categories = tidy['Migration Flow'].cat.categories.map(lambda x: pathify(x))

tidy


# In[200]:


out = Path('out')
out.mkdir(exist_ok=True, parents=True)

tidy.drop_duplicates().to_csv(out / ('observations.csv'), index = False)


# In[201]:


from gssutils.metadata import THEME

scraper.dataset.family = 'migration'
scraper.dataset.theme = THEME['population']
with open(out / 'dataset.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())


# In[ ]:





# In[ ]:




