#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd

covid_data = pd.read_csv('COVID_county_data.csv')
census_data = pd.read_csv('acs2017_census_tract_data.csv')

covid_data.head()


# In[2]:


census_data.head()


# In[6]:


counties = []

for i, row in census_data.iterrows():
    if row['County'] not in counties:
        counties.append(row['County'])
        
#for x in census_data.iloc[census_data['County']]:
 #   if x not in counties:
  #      counties.append(x)
       
#for i in range(len(census_data)):
 #   print(census_data['County'].iloc[i])

#print(counties)

#print(len(counties))


# In[14]:


main_cols = ['County', 'State', 'TotalCases', 'Dec2020Cases', 'TotalDeaths', 'Dec2020Deaths', 'Population', 'Poverty', 'PerCapitaIncome']
main_data = pd.DataFrame(columns = main_cols)
#print(main_data)

census_data.columns
#census_data['IncomePerCap']


# In[20]:


census_cols = ['County', 'State', 'Population', 'Poverty', 'PerCapitaIncome']
census = pd.DataFrame(columns = census_cols)

for county in census_data:
    state = ''
    pop = 0
    poverty = 0
    income = 0
    count = 0
    for i, row in census_data.iterrows():
        if row['County'] == county:
            state = row['State']
            pop += row['TotalPop']
            poverty += row['Poverty']
            income += row['IncomePerCap']
            ++count
    
    if count != 0:
        to_append = pd.DataFrame([county, state, pop, poverty/count, income/count], columns = census_cols)
        census.append(to_append)
            
census.head()


# In[ ]:




