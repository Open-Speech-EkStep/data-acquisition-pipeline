import json
import os


class MediaMetadata:
    def __init__(self):
        self.config_json = self.config_json()['downloader']

    def config_json(self):
        current_path = os.path.dirname(os.path.realpath(__file__))
        config_file = os.path.join(current_path, '..', "configs", "config.json")
        with open(config_file, 'r') as file:
            metadata = json.load(file)
        return metadata

    def create_metadata(self, video_info):
        metadata = {'raw_file_name': video_info['raw_file_name'],
                    'duration': str(video_info['duration']),
                    'title': video_info['raw_file_name'],
                    'speaker_name': self.config_json['speaker_name'] if self.config_json['speaker_name'] else video_info['name'],
                    'audio_id': self.config_json['audio_id'],
                    'cleaned_duration': self.config_json['cleaned_duration'],
                    'num_of_speakers': self.config_json['num_of_speakers'],  # check
                    'language': video_info['language'] if 'language' in video_info else self.config_json['language'],
                    'has_other_audio_signature': self.config_json['has_other_audio_signature'],
                    'type': self.config_json['type'],
                    'source': video_info['source'] if 'source' in video_info else self.config_json['source'],
                    'experiment_use': self.config_json['experiment_use'],  # check
                    'utterances_files_list': self.config_json['utterances_files_list'],
                    'source_url': video_info['source_url'],
                    'speaker_gender': str(self.config_json['speaker_gender']).lower() if self.config_json['speaker_gender'] else
                    video_info['gender'],
                    'source_website': video_info['source_website'] if 'source_website' in video_info else self.config_json[
                        "source_website"],
                    'experiment_name': self.config_json['experiment_name'],
                    'mother_tongue': self.config_json['mother_tongue'],
                    'age_group': self.config_json['age_group'],  # -----------
                    'recorded_state': self.config_json['recorded_state'],
                    'recorded_district': self.config_json['recorded_district'],
                    'recorded_place': self.config_json['recorded_place'],
                    'recorded_date': self.config_json['recorded_date'],
                    'purpose': self.config_json['purpose'],
                    'license': video_info["license"] if "license" in video_info else ""}
        return metadata
