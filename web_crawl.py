import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import spacy  # Ensure spaCy is installed with `pip install spacy`
from collections import Counter
import re

# Load the English language model for spaCy
nlp = spacy.load("en_core_web_sm") #Need to install in addition to spacy module

def extract_names(text):
    """Extract person names from text using spaCy's NER capabilities."""
    doc = nlp(text)
    names = [ent.text for ent in doc.ents if ent.label_ == "PERSON"]
    return Counter(names)  # Return a counter of the most frequent names

def scrape_text_and_links_from_link(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract text from all <p> tags
        text = ' '.join([p.get_text() for p in soup.find_all('p')])

        # Extract person names from the text
        names_counter = extract_names(text)

        if not names_counter:
            return text, []

        # Get the most common names mentioned in the text
        most_common_names = set(name.replace(" ", "").lower() for name in names_counter.keys())
        site_name = re.match(r'(http|https):\/\/(www\.)?(?P<base_url>[a-zA-Z0-9]*)\.', url)['base_url'].lower()
        most_common_names.discard(site_name)


        # Extract links only from <a> tags within <p> tags
        links = []
        for p in soup.find_all('p'):
            for a in p.find_all('a', href=True):
                href = urljoin(url, a['href'])

                # Check if any of the names appear in the URL
                if any(name in href.lower() for name in most_common_names):
                    links.append(href)

        return text, links
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None, []

def get_news_articles(query, num_pages=3):
    news_articles = []

    for page in range(num_pages):
        start = page * 10  # 10 results per page
        # Construct the URL with the custom time range parameter
        url = f"https://www.google.com/search?q={query}&tbs=cdr:1,cd_min:01/01/2000,cd_max:12/31/2004&start={start}"
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
            print(f"Error fetching search results: {e}")

    return news_articles

def crawl_links(starting_url, max_depth=2, visited=None):
    if visited is None:
        visited = set()  # Initialize visited set
        initial_depth = max_depth
    else:
        initial_depth = max_depth+1

    if max_depth < 0 or starting_url in visited:
        return

    visited.add(starting_url)
    print(f'Currently {initial_depth - max_depth} links deep into crawl.')
    text, links = scrape_text_and_links_from_link(starting_url)
    if text:
        # Use the parsed URL to form a filename-safe title
        title = urlparse(starting_url).path.replace('/', '_')
        save_text_to_file(title, text)
        print(f"Text from '{starting_url}' saved to file.")
    else:
        print(f"Failed to scrape text from '{starting_url}'.")

    # Recursively crawl the extracted links
    for link in links:
        time.sleep(2)  # Avoid rate-limiting
        crawl_links(link, max_depth - 1, visited)

def save_text_to_file(title, text):
    filename = "{}.txt".format(title[:50].replace(' ', '_'))  # Truncate title and replace spaces with underscores for filename
    with open(filename, 'w', encoding='utf-8') as file:
        file.write(text)

if __name__ == "__main__":
    query = 'Robert F. Kennedy Jr. education childhood'
    articles = get_news_articles(query, num_pages=1)  # Fetch 1 page of search results
    print(articles)
    for article in articles:
        title = article['title']
        link = article['link']
        
        # Respect robots.txt rules by waiting a few seconds between requests
        time.sleep(2)
        
        # Scrape text from the link
        print(f'Crawling "{title}')
        crawl_links(link)
        
