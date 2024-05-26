import asyncio
import datetime
import os
import time
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
import spacy  # Ensure spaCy is installed with `pip install spacy`
from collections import Counter
import re
from concurrent.futures import ProcessPoolExecutor
import logging
import threading  # For thread-safe operations
import requests

# Define a list of keywords to filter out unwanted links
FILTER_KEYWORDS = ["twitter.com", "x.com", "facebook.com", "mailto:", "intent/tweet"]

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

def extract_structured_text(soup, headers_tags):
    structured_text = []
    all_text = soup.find_all(list(headers_tags.keys()) + ['p'])

    current_header = None
    for element in all_text:
        text = element.get_text()  # Extract text without stripping
        if element.name in headers_tags:
            current_header = headers_tags[element.name] + text.strip()  # Strip whitespace for headers
            structured_text.append((current_header, []))
        elif element.name == 'p' and current_header:
            structured_text[-1][1].append(text.strip())  # Strip whitespace for paragraphs

    return structured_text

def flatten_text(structured_text):
    return '\n\n'.join(['\n'.join([header] + paras) for header, paras in structured_text])

def extract_and_filter_links(soup, url, most_common_names):
    links = set()
    for a in soup.find_all('a', href=True):
        href = urljoin(url, a['href'])
        link_text = a.get_text(strip=True).lower()  # strip and lower case once
        href_lower = href.lower()
        if any(name in href_lower or name in link_text for name in most_common_names):
            if not any(keyword in href_lower for keyword in FILTER_KEYWORDS):
                links.add(href)
    return links

def process_page_content(url, content):
    if not content or not isinstance(content, str):
        logging.error(f"Invalid content fetched from {url}")
        return None, []

    soup = BeautifulSoup(content, 'html.parser')
    headers_tags = {
        'h1': '# ', 'h2': '## ', 'h3': '### ', 'h4': '#### ', 'h5': '##### ', 'h6': '###### '
    }
    
    structured_text = extract_structured_text(soup, headers_tags)
    flat_text = flatten_text(structured_text)
    names_counter = extract_names(flat_text)
    most_common_names = set(name.replace(" ", "").lower() for name in names_counter.keys())
    site_name = re.match(r'(http|https):\/\/(www\.)?(?P<base_url>[a-zA-Z0-9]*)\.', url)['base_url'].lower()
    most_common_names.discard(site_name)

    links = extract_and_filter_links(soup, url, most_common_names)
    logging.info(f'Returning {len(links)} inner links from {url}')

    return flat_text, list(links)

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

def process_contents(urls, contents):
    """Process contents using process pool."""
    with ProcessPoolExecutor() as executor:
        return list(executor.map(process_page_content, urls, contents))

async def scrape_text_and_links(urls):
    """Asynchronously scrape multiple URLs and process their content."""
    headers = {
        "X-No-Cache": "true"
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [fetch(session, url) for url in urls]
        responses = await asyncio.gather(*tasks)

    # Prepare data for processing
    urls_to_process = [resp[0] for resp in responses if resp[1] is not None]
    contents_to_process = [resp[1] for resp in responses if resp[1] is not None]

    # Process content in parallel using a standalone function
    loop = asyncio.get_running_loop()
    results = await loop.run_in_executor(None, process_contents, urls_to_process, contents_to_process)

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
# def modify_url_for_api(url):
#     ind = url.find('.')+1
#     modified_url = url[ind:]
#     # Correctly prefixing the Jina Reader API domain
#     modified_url = 'https://r.jina.ai/' + modified_url
#     return modified_url

def crawl_links(starting_url, current_depth=0, max_depth=2, visited=None):
    """Recursively crawl links, scrape content, and save it."""
    global links_remaining  # Use the global counter

    if visited is None:
        visited = set()  # Initialize visited set

    if current_depth > max_depth or starting_url in visited:
        return

    logging.info(f'Currently {current_depth} links deep into crawl.')
    visited.add(starting_url)

    results = asyncio.run(scrape_text_and_links([starting_url]))
    
    if not results or not results[0]:
        logging.error(f"Failed to scrape text from '{starting_url}'.")
        return

    text, links = results[0]

    if text:
        # Use the parsed URL to form a filename-safe title
        title = urlparse(starting_url).path.replace('/', '_')
        save_text_to_file(title, text)
        logging.info(f"Text from '{starting_url}' saved to file.")
    else:
        logging.error(f"Failed to scrape text from '{starting_url}'.")

    # Check for and handle links count
    if links:
        # Add links to the remaining counter
        links_remaining.release(len(links))
        logging.info(f"Added {len(links)} new links to process from {starting_url}")
    else:
        logging.info(f"No additional links found in '{starting_url}'.")

    # Increment the depth for each recursive call
    new_depth = current_depth + 1

    if new_depth <= max_depth:
        # Recursively crawl the extracted links
        for link in links:
            if max_depth > 0:
                time.sleep(2)  # Avoid rate-limiting
            crawl_links(link, new_depth, max_depth, visited)
        
    # After all recursive calls, now we can safely decrement
    if links:
        links_remaining.acquire(len(links))
        logging.info(f"Finished processing all links from {starting_url}. Remaining links count updated.")

def save_text_to_file(title, text):
    """Save text content to a file."""
    filename = "{}.txt".format(title[:50].replace(' ', '_'))  # Truncate title and replace spaces with underscores for filename
    path = os.path.join(os.getcwd(), 'output', filename)
    with open(path, 'w', encoding='utf-8') as file:
        logging.info(f'Creating file "{path}"')
        file.write(text)

if __name__ == "__main__":
    # query = 'Robert F. Kennedy Jr. education childhood'
    # articles = get_news_articles(query, num_pages=1)  # Fetch 1 page of search results
    # logging.info(f'Gathered {len(articles)} links on "{query}" from Google Search.')
    # logging.info('Initializing web crawling...')
    # for article in articles:
    #     title = article['title']
    #     link = article['link']

    #     # Respect robots.txt rules by waiting a few seconds between requests
    #     time.sleep(2)
        
    #     # Scrape text from the link
    #     logging.info(f'Crawling "{title}"')
    #     crawl_links(link)
    crawl_links('https://www.vanityfair.com/news/story/rfk-jr-says-his-brain-was-partially-eaten-by-a-worm-that-crawled-inside-and-died-everything-you-need-to-know')