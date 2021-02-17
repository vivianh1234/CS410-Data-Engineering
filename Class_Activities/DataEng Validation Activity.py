#!/usr/bin/env python
# coding: utf-8

# In[6]:


import pandas as pd

data = pd.DataFrame(pd.read_csv("Oregon_Hwy_26_Crash_Data_for_2019.csv"))
data.head(10)


# In[17]:


for i in data.itertuples():
    if (data['Vehicle ID'].isna().bool() == True):
        print(f"row {i} does not have a Crash ID")


# In[ ]:




