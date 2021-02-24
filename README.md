# Data Collection Pipeline
### It is a part of [Vakyansh's](https://open-speech-ekstep.github.io/mkdocs/) to enable smart crawling of the web to scrape relevant audio data to train models.

This is downloading framework that is extensible and allows the user to add new source without much code changes. For each new source user need to write a scrapy spider script and rest of downloading and meta file creation is handled by repective pipelines. And if required user can add their custom pipelines. This framework automatically transfer the downloaded data to a Google cloud bucket automatically. For more info on writing scrapy spider and pipeline one can refer to the [documentation](https://docs.scrapy.org/en/latest/intro/tutorial.html). 


<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-12-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v1.4%20adopted-ff69b4.svg)](code-of-conduct.md)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-380/)
[![Scrapy 2.4.0](https://img.shields.io/badge/scrapy-2.4.0-green)](https://scrapy.org/)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-yellow.svg)](https://conventionalcommits.org)


If you like our data collection pipeline_, ⭐ the project to support its development!_

<!-- TABLE OF CONTENTS -->
## Table of Contents

* [About the Project](#about-the-project)
  * [Built With](#built-with)
* [Getting Started](#getting-started)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
* [Usage](#usage)
  * [Setting credentials for Google cloud bucket](#setting-credentials-for-google-cloud-bucket)
  * [Bucket configuration](#bucket-configuration)
  * [Metadata file configurations](#metadata-file-configurations)
  * [Youtube download configurations](#youtube-download-configurations)
  * [Web Crawl Configuraton](#web-crawl-configuration)
  * [Adding new spider](#adding-new-spider)
  * [Running spiders with appropriate pipeline](#running-spiders-with-appropriate-pipeline)
* [Additional services](#additional-services)
  * [Selenium google crawler](#selenium-google-crawler)
  * [Selenium youtube crawler](#selenium-youtube-crawler)
* [Contributing](#contributing)
* [License](#license)
* [Contact](#contact)
* [Acknowledgements](#acknowledgements)



<!-- ABOUT THE PROJECT -->
## About The Project
This is downloading framework that is extensible and allows the user to add new source without much code changes. For each new source user need to write a scrapy spider script and rest of downloading and meta file creation is handled by repective pipelines. And if required user can add their custom pipelines. This framework automatically transfer the downloaded data to a Google cloud bucket automatically. For more info on writing scrapy spider and pipeline one can refer to the [documentation](https://docs.scrapy.org/en/latest/intro/tutorial.html). 

### Built With
We have used scrapy as the base of this framework.
* [Scrapy](https://github.com/scrapy/scrapy)

<!-- GETTING STARTED -->
## Getting Started

To get started install the prerequisites and clone the repo to machine on which you wish to run the framework.

### Prerequisites
1. ffmpeg
* Any linux based (preferred Ubuntu)
```sh
sudo apt-get install ffmpeg
```
* Mac-ox
```sh
brew install ffmpeg
```
2. Supported Python Version = 3.6

* Windows user can follow installation steps on [https://www.ffmpeg.org](https://www.ffmpeg.org)
### Installation

1. Clone the repo
```sh
git clone https://github.com/Open-Speech-EkStep/data-acquisition-pipeline.git
```
2. Install python requirements
```sh
pip install -r requirements.txt
```



<!-- USAGE EXAMPLES -->
## Usage
This framework allows the user to download the media file from a websource(youtube, xyz.com, etc) and creates the respective metadata file from the data that is extracted from the file.For using any added source or to add new source refer to steps below.It can also crawl internet for media of a specific language. For web crawling, refer to the web crawl configuration below.
### Common configuration steps:
#### Setting credentials for Google cloud bucket
You can set credentials for Google cloud bucket in the [credentials.json](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/credentials.json) add the credentials in given manner
```shell script
{"Credentials":{ YOUR ACCOUNT CREDENTIAL KEYS }}
```
#### Bucket configuration
Bucket configurations for data transfer in [storage_config.json](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/data_acquisition_framework/configs/storage_config.json) 
```shell script
"bucket": "ekstepspeechrecognition-dev",          Your bucket name
"channel_blob_path": "scrapydump/refactor_test",  Path to directory where downloaded files is to be stored
"archive_blob_path": "archive",                   Folder name in which history of download is to be maintained
"scraped_data_blob_path": "scraped"               Folder name in which CSV for youtube file mode is stored

Note:
1. Both archive_blob_path and scraped_data_blob_path should be present in channel_blob_path.
2. The CSV file used in file mode of youtube, It\'s name must be same as source_name given above. 
3. (only for datacollector_urls and datacollector_bing spiders) To autoconfigure language parameter to channel_blob_path from web_crawler_config.json, use <language> in channel_blob_path.  
    "eg: for tamil : data/download/<language>/audio - this will replace <language> with tamil."
```
#### Metadata file configurations
Metadata file configurations in [config.json](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/data_acquisition_framework/configs/config.json)
```shell script
mode: 'complete'                        This should not be changed       
audio_id: null                          If you want to give a custom audio id add here 
cleaned_duration: null                  If you know the cleaned duration of audio add here
num_of_speakers: null                   Number of speaker present in audio
language: Hindi                         Language of audio
has_other_audio_signature: False        If audio has multiple speaker in same file (True/False)
type: 'audio'                           Type of media (audio or video)
source: 'Demo_Source'                   Source name
experiment_use: False                   If its for experimental use (True/False)
utterances_files_list: null             
source_website: ''                      Source website url
experiment_name: null                   Name of experiment if experiment_use is True
mother_tongue: null                     Accent of language(Bengali, Marathi, etc...)
age_group: null                         Age group of speaker in audio
recorded_state: null                    State in which audio is recorded
recorded_district: null                 District of state in which audio is recorded
recorded_place: null                    Recording location
recorded_date: null                     Recording date
purpose: null                           Purpose of recording
speaker_gender: null                    Gender of speaker
speaker_name: null                      Name of speaker

Note:
1. If any of the field info is not available keep its value to null
2. If speaker_name or speaker_gender is given then that same will be used for all the files in given source 
```
#### Youtube download configurations
* You can set download mode [file/channel] in [youtube_pipeline_config.py](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/data_acquisition_framework/configs/youtube_pipeline_config.py) 
```shell script
mode = 'file'  # [channel,file]
```
In file mode you will store a csv file whose name must be same as source name in scraped_data_blob_path. csv must  contain urls of youtube videos, speaker name and gender as three different columns. Urls is a must field. You can leave speaker name and gender blank if data is not available. <br>Given below is the structure of csv.</br>
```shell script
 video_url,speaker_name,speaker_gender
https://www.youtube.com/watch?v=K1vW_ZikA5o,Ram_Singh,male
https://www.youtube.com/watch?v=o82HIOgozi8,John_Doe,male
...
```
* common configurations in [youtube_pipeline_config.py](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/data_acquisition_framework/configs/youtube_pipeline_config.py)
```shell script
# Common configurations
"source_name": "DEMO",                            This is the name of source you are downloading
batch_num = 1                                     Number of videos to be downloaded as batches
```
* file mode configurations in [youtube_pipeline_config.py](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/data_acquisition_framework/configs/youtube_pipeline_config.py) 
```shell script
# File Mode configurations
file_speaker_gender_column = 'speaker_gender'     Gender column name in csv file
file_speaker_name_column = "speaker_name"         Speaker name column name in csv file
file_url_name_column = "video_url"                Video url column name in csv file
```
* channel mode configuration in  [youtube_pipeline_config.py](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/youtube/crawler/data_acquisition_framework/configs/youtube_pipeline_config.py)
```shell script
# Channel mode configurations
channel_url_dict = {}             Channel url dictionary (This will download all the videos from the given channels with corresponding source names)
match_title_string = ''       REGEX   Download only matching titles (regex or caseless sub-string)
reject_title_string = ''      REGEX    Skip download for matching titles (regex or caseless sub-string)

Note:
1. In channel_url_dict, the keys must be the urls and values must be their channel names
2. To get list of channels from youtube API, channel_url_dict must be empty
``` 
#### Youtube API configuration
* Automated Youtube fetching configuration in [youtube_api_config.json](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/data_acquisition_framework/configs/youtube_api_config.json)
```shell script
# Youtube API configurations
"language" : "hindi",                             Type of language for which search results are required.
"language_code": "hi",                            Language code for the specified language.
"keywords":[                                      The search keywords to be given in youtube API query
    "audio",
    "speech",
    "talk"
],
"words_to_ignore":[                               The words that are to be ignored in youtube API query
    "song",
    "music"
],
"max_results": 20                                 Maximum number of channels or results that is required.
```
#### Web Crawl Configuration
* web crawl configuration in [web_crawl_config.json](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/data_acquisition_framework/configs/web_crawl_config.py) (Use this only for datacollector_bing and datacollector_urls spider)
```shell script
"language": "gujarati",                           Language to be crawled
"language_code": "gu",                            Language code for the specified language.
"keywords": [                                     Keywords to query
    "talks audio",
    "audiobooks",
    "speeches",
],
"word_to_ignore": [                               Words to ignore while crawling
    "ieeexplore.ieee.org",
    "dl.acm.org",
    "www.microsoft.com"
],
"extensions_to_ignore": [                         Formats/extensions to ignore while crawling
    ".jpeg",
    "xlsx",
    ".xml"
],
"extensions_to_include": [                        Formats/extensions to include while crawling
    ".mp3",
    ".wav",
    ".mp4",
],
"pages": 1,                                       Number of pages to crawl
"depth": 1,                                       Nesting depth for each website
"continue_page": "NO",                            Field to continue/resume crawling
"last_visited": 200,                              Last visited results count
"enable_hours_restriction": "YES",                Restrict crawling based on hours of data collected
"max_hours": 1                                    Maximum hours to crawl
```
#### Adding new spider
As we already mentioned our framework is extensible for any new source. To add a new source user just need to write a spider for that source.<br>To add a spider you can follow the scrapy [documentation](https://docs.scrapy.org/en/latest/intro/tutorial.html) or you can check our [sample](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/data_acquisition_framework/spiders/datacollector_music.py) spider.</br> 

#### Running spiders with appropriate pipeline
* Starting youtube spider with YoutubeApi pipeline.
    * Add youtube search api key in .youtube_api_key file in project root. 
```shell script
scrapy crawl datacollector_youtube --set=ITEM_PIPELINES='{"data_acquisition_framework.pipelines.youtube_api_pipeline.YoutubeApiPipeline": 1}'
```
* Starting datacollector_bing spider with audio pipeline.
```shell script
scrapy crawl datacollector_bing
```
* Starting datacollector_urls spider with audio pipeline.
Make sure to put the urls to crawl in the data_acquisition_framework/urls.txt
```shell script
scrapy crawl datacollector_urls
```

## Additional Services

#### Selenium google crawler

* It is capable of crawling search results of google for a given language and exporting them to urls.txt file. This urls.txt file can be used with datacollector_urls spider to crawl all the search results website and download the media along with their metadata.

* A specified Readme can be found in selenium_google_crawler folder. [Readme for selenium google crawler](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/selenium_google_crawler/Readme.md)


#### Selenium youtube crawler

* It is capable of crawling youtube videos using youtube api or from a list of files with youtube video ids provided with channel name as filename.

* A specified Readme can be found in selenium_youtube_crawler folder. [Readme for selenium youtube crawler](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/selenium_youtube_crawler/Readme.md)


<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to be learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request



<!-- LICENSE -->
## License

Distributed under the [XYZ] License. See `LICENSE` for more information.



<!-- CONTACT -->
## Contact

Your Name - [@your_twitter](https://twitter.com/your_username) - email@example.com

Project Link: [https://github.com/your_username/repo_name](https://github.com/your_username/repo_name)



<!-- ACKNOWLEDGEMENTS -->
## Acknowledgements
* [Scrapy](https://github.com/scrapy/scrapy)
* [YouTube-dl](https://github.com/ytdl-org/youtube-dl)
* [TinyTag](https://github.com/devsnd/tinytag)
