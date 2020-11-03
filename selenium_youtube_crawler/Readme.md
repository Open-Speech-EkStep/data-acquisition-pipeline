# Youtube Crawler

This crawler can be used to crawl youtube videos and upload them to google cloud bucket along with a metadata file.

This crawler can be configured by modified the parameter values of config.json.

## Steps:

    1. Run pip install -r requirements.txt.
    2. Make required changes to config.json.
    3. python crawl_youtube.py

## Configuration parameters:

    1. language - the type of language for which search results are required.
    2. language_code - language code for the specified language.
    3. keywords - the search keyword to be given in youtube API query.
    4. words_to_ignore - can be used to ignore urls that has the given words.
    5. max_results - maximum number of channels or results that is required.
    6. bucket_name - name of the gcp bucket.
    7. bucket_path - path where to store the file.
    8. input_type - type of input to crawler.. values = [FILE or YOUTUBE_API]

## Note:
    1. If input_type is file, then create a folder named playlists and add files with channel name as filename and channel videos ids as file content.
    
    eg. playlist/Tamil_Channel.txt having content
            
        lajvqyWWfnk
        sbOsG0ytCQg
            

    2. If input_type is YOUTUBE_API, then create a .env file with youtube search api key as below:
    
        youtube_api_key=<your api key>
