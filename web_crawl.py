import asyncio
import datetime
import os
import time
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import spacy  # Ensure spaCy is installed with `pip install spacy`
from collections import Counter
import re
from concurrent.futures import ProcessPoolExecutor
import logging
import threading  # For thread-safe operations
import requests

# Global counter for links remaining
links_remaining = threading.Semaphore(0)  # Initialize with zero count

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load the English language model for spaCy
nlp = spacy.load("en_core_web_sm")  # Need to install this in addition to the spaCy module

def extract_names(text):
    """Extract person names from text using spaCy's NER capabilities."""
    doc = nlp(text)
    names = [ent.text for ent in doc.ents if ent.label_ == "PERSON"]
    return Counter(names)  # Return a counter of the most frequent names

def process_page_content(url, content):
    """Process the content of a page to extract text, links, and filter them."""
    soup = BeautifulSoup(content, 'html.parser')

    # Extract text from all <p> tags
    text = ' '.join([p.get_text() for p in soup.find_all('p')])

    # Extract person names from the text
    names_counter = extract_names(text)
    # print('NAMES:',names_counter)
    if not names_counter:
        return text, []

    # Normalize and filter names
    most_common_names = set(name.replace(" ", "").lower() for name in names_counter.keys())
    site_name = re.match(r'(http|https):\/\/(www\.)?(?P<base_url>[a-zA-Z0-9]*)\.', url)['base_url'].lower()
    most_common_names.discard(site_name)
    # print('Most Common Names:', most_common_names)

    # Extract links only from <a> tags within <p> tags
    links = []
    for p in soup.find_all('p'):
        for a in p.find_all('a', href=True):
            href = urljoin(url, a['href'])

            # Check if any of the names appear in the URL
            if any(name in href.lower() for name in most_common_names):
                links.append(href)

    logging.info(f'Returning {len(links)} Inner Links from {url}')
    return text, links

async def fetch(session, url):
    """Fetch a URL asynchronously."""
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            content = await response.text()
            return url, content
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
        return url, None

async def scrape_text_and_links(urls):
    """Asynchronously scrape multiple URLs and process their content."""
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, url) for url in urls]
        responses = await asyncio.gather(*tasks)

    # Process content in parallel using a standalone function
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(process_page_content, [resp[0] for resp in responses], [resp[1] for resp in responses]))
        # print()
        # print (results)
        # print()
    return results

def get_news_articles(query: str, start_date: str = None, end_date: str = None, num_pages: int = 3):
    """
    Fetches search results from Google for a given query, number of pages,
    and optionally a date range.

    Parameters:
        query (str): The search query.
        num_pages (int): The number of pages to fetch.
        start_date (str): The start date in YYYY-MM-DD format.
        end_date (str): The end date in YYYY-MM-DD format.

    Returns:
        list[dict]: A list of dictionaries containing article titles and links.
    """

    news_articles = []
    try:
        # Convert start_date and end_date to the appropriate format for Google search
        if start_date is not None:
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').strftime('%m/%d/%Y')
        
        if end_date is not None:
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').strftime('%m/%d/%Y')
    except Exception as e:
        logging.error(f"Error converting date start and end dates '{start_date}' and '{end_date}': {e}")
        return None
    

    for page in range(num_pages):
        start = page * 10  # 10 results per page

        # Construct the URL with optional date range parameters
        if start_date and end_date:
            url = f"https://www.google.com/search?q={query}&tbs=cdr:1,cd_min:{start_date},cd_max:{end_date}&start={start}"
        else:
            url = f"https://www.google.com/search?q={query}&start={start}"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            for item in soup.select('.tF2Cxc'):
                title = item.select_one('.DKV0Md').text
                link = item.a['href']
                news_articles.append({'title': title, 'link': link})
        except Exception as e:
            logging.error(f"Error fetching search results: {e}")

    return news_articles

def crawl_links(starting_url, max_depth=2, visited=None):
    """Recursively crawl links, scrape content, and save it."""
    global links_remaining  # Use the global counter

    if visited is None:
        visited = set()  # Initialize visited set
        initial_depth = max_depth
    else:
        initial_depth = max_depth +1

    if max_depth < 0 or starting_url in visited:
        return

    logging.info(f'Currently {initial_depth - max_depth} links deep into crawl.')
    visited.add(starting_url)

    results = asyncio.run(scrape_text_and_links([starting_url]))
    text, links = results[0]

    if text:
        # Use the parsed URL to form a filename-safe title
        title = urlparse(starting_url).path.replace('/', '_')
        save_text_to_file(title, text)
        logging.info(f"Text from '{starting_url}' saved to file.")
    else:
        logging.error(f"Failed to scrape text from '{starting_url}'.")

    # Check for and handle links count
    if len(links) > 0:
        # Add links to the remaining counter
        links_remaining.release(len(links))
    else:
        logging.info(f"No additional links found in '{starting_url}'.")

    # Recursively crawl the extracted links
    for link in links:
        time.sleep(2)  # Avoid rate-limiting
        crawl_links(link, max_depth - 1, visited)
        # Decrease remaining links count and log progress
        links_remaining.acquire(1)
        logging.info(f"Links remaining: {links_remaining._value}")

def save_text_to_file(title, text):
    """Save text content to a file."""
    filename = "{}.txt".format(title[:50].replace(' ', '_'))  # Truncate title and replace spaces with underscores for filename
    path = os.path.join(os.getcwd(), 'output', filename)
    with open(path, 'w', encoding='utf-8') as file:
        file.write(text)

if __name__ == "__main__":
    query = 'Robert F. Kennedy Jr. education childhood'
    articles = get_news_articles(query, num_pages=1)  # Fetch 1 page of search results
    logging.info(f'Gathered {len(articles)} links on "{query}" from Google Search.')
    logging.info('Initializing web crawling...')
    for article in articles:
        title = article['title']
        link = article['link']

        # Respect robots.txt rules by waiting a few seconds between requests
        time.sleep(2)
        
        # Scrape text from the link
        logging.info(f'Crawling "{title}"')
        crawl_links(link)
