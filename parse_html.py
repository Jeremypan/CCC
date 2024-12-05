from bs4 import BeautifulSoup
html_file=open('language_html_table','r',encoding='utf-8')
soup=BeautifulSoup(html_file,'lxml')
data=soup.select('td')[2:]
language={}
for i in range(0,len(data),2):
    language[data[i+1].string]=data[i].string
print(language)