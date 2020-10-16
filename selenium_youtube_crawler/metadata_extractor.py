import moviepy.editor
import pandas as pd

def get_config():
    return {
        'mode': 'complete',                           
        'audio_id': None,
        'cleaned_duration': None,
        'num_of_speakers': None,
        'language': 'Tamil',
        'has_other_audio_signature': False,
        'type': 'audio',
        'source': 'CRAWL_SOURCE_TAMIL',
        'experiment_use': False,
        'utterances_files_list': None,
        'source_website': 'https://www.youtube.com',
        'experiment_name': None,
        'mother_tongue': None,
        'age_group': None,
        'recorded_state': None,
        'recorded_district': None,
        'recorded_place': None,
        'recorded_date': None,
        'purpose': None,
        'speaker_gender': None,
        'speaker_name': None
    }

def create_metadata(video_info, yml):
    metadata = {'raw_file_name': video_info['raw_file_name'],
                'duration': str(video_info['duration']),
                'title': video_info['raw_file_name'],
                'speaker_name': yml['speaker_name'] if yml['speaker_name'] else video_info['name'],
                'audio_id': yml['audio_id'],
                'cleaned_duration': yml['cleaned_duration'],
                'num_of_speakers': yml['num_of_speakers'],  # check
                'language': yml['language'],
                'has_other_audio_signature': yml['has_other_audio_signature'],
                'type': yml['type'],
                'source': yml['source'],  # --------
                'experiment_use': yml['experiment_use'],  # check
                'utterances_files_list': yml['utterances_files_list'],
                'source_url': video_info['source_url'],
                'speaker_gender': str(yml['speaker_gender']).lower() if yml['speaker_gender'] else video_info['gender'],
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

def create_metadata_for_audio(video_info, yml, source):
    metadata = create_metadata(video_info, yml)
    metadata["source"] = source
    return metadata

def extract_metadata(file_dir, file, url, source):
    duration_in_seconds = 0
    video_info = {}
    video_duration = 0
    FILE_FORMAT = file.split('.')[-1]
    meta_file_name = file.replace(FILE_FORMAT, "csv")
    source_url = url
    if FILE_FORMAT == 'mp4':
        video = moviepy.editor.VideoFileClip(file_dir+"/"+file)
        duration_in_seconds = int(video.duration)
        video_duration = duration_in_seconds / 60
    video_info['duration'] = video_duration
    video_info['raw_file_name'] = file
    video_info['name'] = None
    video_info['gender'] = None
    video_info['source_url'] = source_url
    metadata = create_metadata_for_audio(video_info, get_config(), source)
    metadata_df = pd.DataFrame([metadata])
    metadata_df.to_csv(file_dir+"/"+meta_file_name, index=False)
    return duration_in_seconds
