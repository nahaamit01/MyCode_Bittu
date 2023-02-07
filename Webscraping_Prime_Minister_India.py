#!/usr/bin/env python
# coding: utf-8

# In[208]:


from bs4 import BeautifulSoup
import pandas as pd
import requests

url ="https://en.wikipedia.org/wiki/List_of_prime_ministers_of_India"

res=requests.get(url).text
soup=BeautifulSoup(res,"html.parser")

pm_india=soup.find("table",{"class":"wikitable"})

print(pm_india)


# In[209]:


df=pd.read_html(str(pm_india))

df=pd.DataFrame(df[0])
df


# In[210]:


df.columns=['No','Portrait','Name','Constituency','Took_office','Left_office','Time_in_office','Lok_Sabha','Ministry','Appointed_by','Party','untitle']


# In[211]:


df.head(26) 


# In[212]:


# df["Left_office"].mask(df["Leftoffice"]==('10 November 1990[NC]','10 November 1990',inplace=True)
# df["Leftoffice"].mask(df["Leftoffice"]==('10 October 1999[NC]','10 October 1999',inplace=True)


# In[213]:


df2=df[["No","Name","Took_office","Left_office","Time_in_office"]]
df2


# In[214]:


df["Left_office"]=df["Left_office"].str.replace("†","")
df["Took_office"]=df["Took_office"].str.replace("†","")


# In[215]:


df["Left_office"]=df["Left_office"].str.replace("[RES]","")
df["Left_office"]=df["Left_office"].str.replace("[NC]","")
df["Took_office"]=df["Took_office"].str.replace("[§]","")


# In[216]:


df["Left_office"]=df["Left_office"].str.replace("[[]]","")
df["Took_office"]=df["Took_office"].str.replace("[[]]","")


# In[217]:


df['Left_office']=df["Left_office"].str.replace("10 ovember 1990","10 November 1990")
df['Took_office']=df["Took_office"].str.replace("10 ovember 1990","10 November 1990")


# In[218]:


df[["Name","Party","Took_office","Left_office","Time_in_office"]]


# In[219]:


df1=df[["Name","Party","Took_office","Left_office","Time_in_office"]]


# In[220]:


df2=df1.drop(26)


# In[221]:


df2


# In[222]:


df2["Left_office"] = pd.to_datetime(df2["Left_office"],infer_datetime_format=True) 
df2["Took_office"] = pd.to_datetime(df2["Took_office"],infer_datetime_format=True)
df2


# In[223]:


df2['Tenure'] = df2['Left_office'] - df2['Took_office'] 
df2


# In[ ]:


df2.to_csv('Prime_Minister_Csv_Chart')
Prime_Minister_Csv_Chart = pd.read_csv(r"/content/Prime_Minister_Csv_Chart",index_col=0)
Prime_Minister_Csv_Chart


# In[224]:


Group = df2['Tenure'].groupby(df2['Name']).mean()
Group = pd.DataFrame(Group)
Group


# In[225]:


df3=Group.sort_values(by='Tenure',ascending=False)
df3


# In[226]:


print(df3.iloc[2])


# In[227]:


total_time={}
for x in df["Name"]:
  Add=0
  for y in df["Name"]:
    if x == y:
      Add+=1
      total_time[x]=Add

T={"Name":[],'Times':[]}
for x in total_time:
  T['Name'].append(x)
  T['Times'].append(total_time[x])


# In[228]:


M=pd.DataFrame.from_dict(T)
M


# In[ ]:


get_ipython().system('pip install pyodbc')


# In[ ]:


pip install pymysql


# In[ ]:


from sqlalchemy import create_engine
c=create_engine("mysql+pymysql://root:Amit#123@127.0.0.1:3306/test1")
df4.to_sql("pm_india",c,if_exists='replace',index=False)

