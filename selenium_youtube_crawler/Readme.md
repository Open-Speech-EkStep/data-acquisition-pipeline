# Youtube Crawler

This crawler can be used to crawl youtube videos and upload them to google cloud bucket along with a metadata file.

This crawler can be configured by modified the parameter values of config.json.

Read the note before proceeding with running this service.

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
1. If input_type is file, videos for channels have to be given in a local folder.There are two ways to do this:
    - Create a folder named `playlists` and add files with channel name as filename and channel videos ids as file content.
        ```
        eg. playlists/Tamil_Channel.txt having content
                
            lajvqyWWfnk
            sbOsG0ytCQg
        ```
    - To automatically generate this content(configure the config.json file for your requirements), Run the following command:
        ```
            python youtube_util.py
        ```    

2. If input_type is `YOUTUBE_API`, then create a `.youtube_api_key` file in this folder and get API KEY from google developer console and put it in this file.
