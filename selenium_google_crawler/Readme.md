# Google Crawler

This crawler can be used to crawl search engine results of google for a list of search entries.

This crawler can be configured by modified the parameter values of config.json.

`Output of this crawler(urls.txt) can be given to datacollector_urls spider in data_acquisition_framework folder.`

## Steps:

    1. Run pip install -r requirements.txt.
    2. Make required changes to config.json.
    3. python crawl_google.py
    4. urls.txt file will be generated as a result with crawled urls.

## Configuration parameters:

    1. language - the type of language for which search results are required.
    2. keywords - the search keyword to be given in google search engine.
    3. words_to_ignore - can be used to ignore urls that has the given words.
    4. extensions_to_ignore - used to ignore having given extensions.
    5. max_pages - maximum number of pages that can fetched for a single keyword(maximum but not exact number of pages; can be lesser as well).

