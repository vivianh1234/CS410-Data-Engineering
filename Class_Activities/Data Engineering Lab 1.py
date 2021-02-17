#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
get_ipython().run_line_magic('matplotlib', 'inline')


# In[5]:


from urllib.request import urlopen
from bs4 import BeautifulSoup

url = "http://www.hubertiming.com/results/2017GPTR10K"
html = urlopen(url)

soup = BeautifulSoup(html, 'lxml')
type(soup)


# In[6]:


# Get the title
title = soup.title
print(title)


# In[7]:


# Print out the text
text = soup.get_text()
print(soup.text)


# In[8]:


soup.find_all('a')


# In[9]:


all_links = soup.find_all("a")
for link in all_links:
    print(link.get("href"))


# In[10]:


# Print the first 10 rows for sanity check
rows = soup.find_all('tr')
print(rows[:10])


# In[11]:


for row in rows:
    row_td = row.find_all('td')
print(row_td)
type(row_td)


# In[12]:


str_cells = str(row_td)


# In[13]:


print(str_cells)


# In[14]:


cleantext = BeautifulSoup(str_cells, "lxml").get_text()
print(cleantext)


# In[15]:


list_rows = []
for row in rows:
    cells = row.find_all('td')
    str_cells = str(cells)
    clean = re.compile('<.*?>')
    clean2 = (re.sub(clean, '',str_cells))
    list_rows.append(clean2)
print(clean2)
type(clean2)


# In[16]:


df = pd.DataFrame(list_rows)
df.head(10)


# In[19]:


df1 = df[0].str.split(',', expand=True)
df1.head(10)


# In[22]:


df1[0] = df1[0].str.strip('[')
df1.head(10)


# In[23]:


col_labels = soup.find_all('th')
all_header = []
col_str = str(col_labels)
cleantext2 = BeautifulSoup(col_str, "lxml").get_text()
all_header.append(cleantext2)
print(all_header)


# In[26]:


df2 = pd.DataFrame(all_header)
df2.head()


# In[27]:


df3 = df2[0].str.split(',', expand=True)
df3.head()


# In[28]:


frames = [df3, df1]
df4 = pd.concat(frames)
df4.head(10)


# In[29]:


df5 = df4.rename(columns=df4.iloc[0])
df5.head()


# In[30]:


df5.info()
df5.shape


# In[31]:


df6 = df5.dropna(axis=0, how='any')
df6.info()
df6.shape


# In[33]:


df7 = df6.drop(df6.index[0])
df7.head(10)


# In[34]:


df7.rename(columns={'[Place': 'Place'},inplace=True)
df7.rename(columns={' Team]': 'Team'},inplace=True)
df7.head()


# In[35]:


df7['Team'] = df7['Team'].str.strip(']')
df7.head()


# In[45]:


#cleanname = re.compile('<[\\r\\n]*>')
#df7[' Name'] = df7[' Name'].sub(cleanname, '',df7[' Name'])
#df8 = df7.replace(to_replace = '[\\r\\n]*'. value='', regex = True)

df7[' Name'] = df7[' Name'].str.lstrip()
df7[' Name'] = df7[' Name'].str.rstrip()
df7['Team'] = df7['Team'].str.lstrip()
df7['Team'] = df7['Team'].str.rstrip()
df7.head()


# In[61]:


time_list = df7[' Chip Time'].tolist()
time_mins = []
for i in time_list:
    #print(i)
    if len(i) > 6:
        h, m, s = i.split(':')
        math = (int(h) * 3600 + int(m) * 60 + int(s))/60
    else:
        m, s = i.split(':')
        math = m
    time_mins.append(math)
print(time_mins)


# In[71]:


for i in time_mins:
    i = int(i)
df7['Runner_mins'] = time_mins
df7.head()


# In[73]:


df7['Runner_mins'].describe(include=[np.number])
#df7.describe(include=[np.number])
#df7.describe()


# In[ ]:




