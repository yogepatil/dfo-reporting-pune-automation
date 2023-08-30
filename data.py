from atlassian import Confluence
import requests as rq
from bs4 import BeautifulSoup
import pandas as pd

dd = Confluence(url='https://tlvconfluence01.nice.com/',
           username='obartakke', password='SOlaris@2022')

spacex = 'IN'
titlex = "Table_Metadata_Validation"


# print(dd.get_page_id(space=spacex, title=titlex))
# print(dd.page_exists(space=spacex,title=titlex))
# print(dd.get_subtree_of_content_ids(1047917090))
# print('omkar')
# print(dd.get_page_properties(1047917090))
# for item in dd.get_child_pages(1047917090):
#     print(item)


dd = rq.get('https://tlvconfluence01.nice.com/display/IN/Table_Metadata_Validation')
ss = BeautifulSoup(dd.text, 'html.parser')
table_find = ss.find('table', class_ = "relative-table confluenceTable")

print(ss)