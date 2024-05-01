# basic-web-crawler
Basic Web Crawler that takes a prompt to query Google search and pulls 'n' number of article files, saving them locally with the option to crawl links within the files.

## Setup and Running Code
1. Clone the repo locally
```sh
git clone https://github.com/nashdean/basic-web-crawler.git
```
2. Run env.sh to initialize your virtual python environment and install the necessary dependencies
```sh
./env.sh
```
3. Modify the search `query` variable in web_crawl.py to whatever you would like to pull articles from
- Defaults to `What is web crawling?` if `query` is missing

4. Run the Python script from the Project directory
```sh
python web_crawl.py
```