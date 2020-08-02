import os
import yaml


def config_yaml():
    config_path = os.path.dirname(os.path.realpath(__file__))
    print(os.listdir(config_path))
    config_file = config_path + '/config.py'
    config_yaml_file = config_file.replace('.py', '.yaml')
    os.rename(config_file, config_yaml_file)
    with open(config_yaml_file) as file:
        yml = yaml.load(file, Loader=yaml.FullLoader)
    return yml


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
