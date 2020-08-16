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
  * [Adding new spider](#adding-new-spider)
  * [Running spiders with appropriate pipeline](#running-spiders-with-appropriate-pipeline)
* [Contributing](#contributing)
* [License](#license)
* [Contact](#contact)
* [Acknowledgements](#acknowledgements)



<!-- ABOUT THE PROJECT -->
## About The Project
This is downloading framework that is extensible and allows the user to add new source without much code changes. For each new source user need to write a scrapy spider script and rest of downloading and meta file creation is handled by repective pipelines. And if required user can add their custom pipelines. This framework automatically transfer the downloaded data to a Google cloud bucket automatically. For more info on writing scrapy spider and pipeline one can refer to the [documemtation](https://docs.scrapy.org/en/latest/intro/tutorial.html). 

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
This framework allows the user to download the media file from a websource(youtube, xyz.com, etc) and creates the respective metadata file from the data that is extracted from the file. For using any added source or to add new source refer to steps below.
### Common configuration steps:
#### Setting credentials for Google cloud bucket
You can set credentials for Google cloud bucket in the [credentials.json](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/credentials.json) add the credentials in given manner
```shell script
{"Credentials":{ YOUR ACCOUNT CREDENTIAL KEYS }}
```
#### Bucket configuration
Bucket configurations for data transfer in [pipeline_config.py](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/data_acquisition_framework/pipeline_config.py) 
```shell script
bucket = ''                     Your bucket name
channel_blob_path = ''          Path to directory where downloaded files is to be stored
archive_blob_path = ''          Folder name in which history of download is to be maintained
source_name = ''                This is the name of source you are downloading
batch_num =                     Number of files fo download in a batch
scraped_data_blob_path = ""     Folder name in which CSV for youtube file mode is stored

Note:
1. Both archive_blob_path and scraped_data_blob_path should be present in channel_blob_path.
2. The CSV file used in file mode of youtube, It's name must be same as source_name given above. 
```
#### Metadata file configurations
Metadata file configurations in [config.py](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/data_acquisition_framework/config.py)
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
1. If any of the field ifo is not available keep its value to null
2. If speaker_name or speaker_gender is given then that same will ve used for all the files in given source 
```
#### Youtube download configurations
* You can set download mode [file/channel] in [pipeline_config.py](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/data_acquisition_framework/pipeline_config.py) 
```shell script
mode = 'file'  # [channel,file]
```
In file mode you will store a csv file whose name must be same as source name in scraped_data_blob_path. csv must  contain urls of youtube videos, speaker name and gender as three different columns. Urls is a must field. You can leave speaker name and gender blank if data is not available. <br>Given below is the structure of csv.</br>
```sh
 video_url,speaker_name,speaker_gender
https://www.youtube.com/watch?v=K1vW_ZikA5o,Ram_Singh,male
https://www.youtube.com/watch?v=o82HIOgozi8,John_Doe,male
...
```
* file mode configurations in [pipeline_config.py](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/data_acquisition_framework/pipeline_config.py) 
```shell script
# File Mode configurations
file_speaker_gender_column = 'speaker_gender'     Gender column name in csv file
file_speaker_name_column = "speaker_name"         Speaker name column name in csv file
file_url_name_column = "video_url"                Video url column name in csv file
```
* channel mode configuration in  [pipeline_config.py](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/data_acquisition_framework/pipeline_config.py)
```shell script
# Channel mode configurations
channel_url = ''              Channel url (This will download all the videos from the given channel)
match_title_string = ''       REGEX   Download only matching titles (regex or caseless sub-string)
reject_title_string = ''      REGEX    Skip download for matching titles (regex or caseless sub-string)
``` 
#### Adding new spider
As we already mentioned aur framework is extensible for any new source. To add a new source user just need to write a spider for that source.<br>To add a spider you can follow the scrapy [documemtation](https://docs.scrapy.org/en/latest/intro/tutorial.html) or you can check our [sample](https://github.com/Open-Speech-EkStep/data-acquisition-pipeline/blob/master/data_acquisition_framework/spiders/datacollector_music.py) spider.</br> 

#### Running spiders with appropriate pipeline
* Starting youtube spider with Youtube pipeline.
```shell script
scrapy crawl datacollector_youtube --set=ITEM_PIPELINES={'data_acquisition_framework.pipelines.YoutubePipeline': 1}
```
Note: You can download youtube video using youtube pipeline only.
* Starting datacollector_music spider with media pipeline.
```shell script
scrapy crawl datacollector_music --set=ITEM_PIPELINES={'data_acquisition_framework.pipelines.MediaPipeline': 1}
```
Note: You can use media pipeline for any other website source or you can write your own pipeline.
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