"""
Module with all logic for scraping ingame updates

First, there is a function that generates the appropriate URL.
Subsequently, this page is accessed to obtain the actual game update info

"""
import datetime

import requests

from typing import List, Optional, Tuple
from bs4 import BeautifulSoup

# <a class= href="https://secure.runescape.com/m=news/sailing-alpha-live-now?oldschool=1">Sailing Alpha Live Now!</a>
_news_article_element_from_overview = {"class": "news-list-article__title-link"}
_next_news = {"id": "nextNews", "class": "news-archive-next"}
_dtn = datetime.datetime.now()


# https://secure.runescape.com/m=news/archive?oldschool=1&year=2025&month=3
def get_articles(year: int, month: int) -> List[str]:
    """Get all articles listed on an OSRS news page defined by `year` and `month`"""
    if year > _dtn.year or year == _dtn.year and month > _dtn.month:
        return []
    
    url = f"""https://secure.runescape.com/m=news/archive?oldschool=1&year={year}&month={month}"""
    bs = BeautifulSoup(requests.get(url).content, "html.parser")
    url_list = [el['href'] for el in bs.find_all("a", attrs=_news_article_element_from_overview)]
    return url_list


def extract_article(url: str):
    """Extract the article found at `url`"""
    bs = BeautifulSoup(requests.get(url).content, "html.parser")
    header = bs.find('div', {"id": "osrsArticleHolder", "class": "news-article-header"})
    title = header.find('h2').text
    publish_date = datetime.datetime.strptime(header.find('time', {"class": "news-article-header__date"})["datetime"], "%Y-%m-%d")
    


urls = get_articles(2025, 4)

for u in urls:
    print(u)
...