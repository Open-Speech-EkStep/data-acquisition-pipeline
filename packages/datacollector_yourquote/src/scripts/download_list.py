# -*- coding: utf-8 -*-

import pandas as pd
import yaml
import time
import pytube
import random


def config_yaml(config):
    with open(config) as file:
        documents = yaml.load(file, Loader=yaml.FullLoader)
    return documents


def create_metadata(video_info, yml):
    metadata = {'raw_file_name': video_info['raw_file_name'],
                'duration': str(video_info['duration']),
                'title': video_info['raw_file_name'],
                'speaker_name': video_info['name'],
                'audio_id': yml['audio_id'],
                'cleaned_duration': yml['cleaned_duration'],
                'num_of_speakers': yml['num_of_speakers'],  # check
                'language': yml['language'],
                'has_other_audio_signature': yml['has_other_audio_signature'],
                'type': yml['type'],
                'source': yml['source'],  # --------
                'experiment_use': yml['experiment_use'],  # check
                'utterances_file_list': yml['utterances_file_list'],
                'source_url': video_info['source_url'],
                'speaker_gender': video_info['gender'],
                'source_website': yml['source_website'],  # --------
                'experiment_name': yml['experiment_name'],
                'mother_tongue': yml['mother_tongue'],
                'age_group': yml['age_group'],  # -----------
                'recorded_state': yml['recorded_state'],
                'recorded_district': yml['recorded_district'],
                'recorded_place': yml['recorded_place'],
                'recorded_date': yml['recorded_date'],
                'purpose': yml['purpose']}
    return metadata


def download_video(video_url, video_name, output_path):
    try:
        audio_info = {}
        yt = pytube.YouTube(video_url)
        audio_info['duration'] = yt.length / 60
        audio_info['raw_file_name'] = video_name
        audio_info['source_url'] = video_url

        print("Downloading video: {}".format(video_name))
        video_path=yt.streams.filter(only_audio=True)[0].download(output_path=output_path, filename=str(video_name))
        print("Downloaded video: {} successfully".format(video_name))
        video_name = video_name + '.mp4'
        #video_path = output_path + '/' + video_name
        print("Generating metadata file...")
        return video_path, audio_info
    except:
        print("Connection error at video ", video_url)
        return None,None


if __name__ == "__main__":
    config_file = "../resources/config.yaml"
    output_path = "../data/raw/"
    df = pd.read_csv("../resources/processed_output/yourquote_processed.csv")
    links = [link.replace("\n", "") for link in df["hind_video_links"]]
    speaker = [each_speaker for each_speaker in df['speaker_names']]
    video_title = [title for title in df["hindi_video_title"]]
    yml_config = config_yaml(config_file)['downloader']
    no_of_records = len(links)
    broken_links = []
    batch_size = 5
    iteration_count = 0
    start = int(input("Enter starting point"))
    split_size = 0
    for record_no in range(start, no_of_records):
        print('\nFile processing status [{x}/{y}]'.format(x=record_no, y=no_of_records))
        file_path, audio_info = download_video(df["hind_video_links"][record_no], video_title[record_no], output_path)
        if file_path == None and audio_info== None:
            # input("\nError downloading audio...  ENTER")
            broken_links.append(df["hind_video_links"][record_no])
            dfs = pd.DataFrame({'broken_links': broken_links})
            dfs.to_csv('../data/broken_links/broken_links{start}-{end}.csv'.format(start=start, end=record_no), index=False)
            continue
        audio_info['name'] = speaker[record_no]
        audio_info['gender'] = None
        audio_info['source_url'] = links[record_no]

        metadata = create_metadata(audio_info, yml_config)

        metadata_df = pd.DataFrame([metadata])
        metadata_df.to_csv(file_path.replace('mp4', 'csv'), index=False)
        split_size += 1
        iteration_count += 1
        if iteration_count == 2:
                    sleep = random.choice([3, 5, 7, 9, 10, 12, 13, 14, 15, 17,22,27,29,34,36,49,55,60])
                    print('\nSleeping for ' + str(sleep) + ' Seconds')
                    time.sleep(sleep)
                    iteration_count = 0
        if split_size == 883:
            print("##-----------------------------------broken_links--------------------------------------##")
            print(broken_links)
            print("##-------------------------------------------------------------------------------##")
            flag = input("\ncountinue(yes) :\nquit(no) :").lower()
            if flag == 'no':
                break
            else:
                split_size = 0
# dfs = pd.DataFrame({'broken_links': broken_links})
# dfs.to_csv('./batch/broken_links{start}-{end}.csv'.format(start=start, end=record_no), index=False)
print('Download complete.......')
