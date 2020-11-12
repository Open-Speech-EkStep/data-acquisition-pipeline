import json
import os


class MediaMetadata:
    def __init__(self):
        current_path = os.path.dirname(os.path.realpath(__file__))
        config_file = os.path.join(current_path, '..', "configs", "config.json")
        with open(config_file, 'r') as file:
            config_json = json.load(file)["downloader"]
        self.config_json = config_json

    def create_metadata(self, media_info):
        metadata = {'raw_file_name': media_info['raw_file_name'],
                    'duration': str(media_info['duration']),
                    'title': media_info['raw_file_name'],
                    'speaker_name': self.config_json['speaker_name'] if self.config_json['speaker_name'] else media_info['name'],
                    'audio_id': self.config_json['audio_id'],
                    'cleaned_duration': self.config_json['cleaned_duration'],
                    'num_of_speakers': self.config_json['num_of_speakers'],  # check
                    'language': media_info['language'] if 'language' in media_info else self.config_json['language'],
                    'has_other_audio_signature': self.config_json['has_other_audio_signature'],
                    'type': self.config_json['type'],
                    'source': media_info['source'] if 'source' in media_info else self.config_json['source'],
                    'experiment_use': self.config_json['experiment_use'],  # check
                    'utterances_files_list': self.config_json['utterances_files_list'],
                    'source_url': media_info['source_url'],
                    'speaker_gender': str(self.config_json['speaker_gender']).lower() if self.config_json['speaker_gender'] else
                    media_info['gender'],
                    'source_website': media_info['source_website'] if 'source_website' in media_info else self.config_json[
                        "source_website"],
                    'experiment_name': self.config_json['experiment_name'],
                    'mother_tongue': self.config_json['mother_tongue'],
                    'age_group': self.config_json['age_group'],  # -----------
                    'recorded_state': self.config_json['recorded_state'],
                    'recorded_district': self.config_json['recorded_district'],
                    'recorded_place': self.config_json['recorded_place'],
                    'recorded_date': self.config_json['recorded_date'],
                    'purpose': self.config_json['purpose'],
                    'license': media_info["license"] if "license" in media_info else ""}
        return metadata
