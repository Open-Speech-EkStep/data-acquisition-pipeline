from unittest import TestCase
from unittest.mock import patch

from data_acquisition_framework.metadata.metadata import MediaMetadata


class TestMediaMetadata(TestCase):
    @patch('data_acquisition_framework.metadata.metadata.load_config_file')
    def setUp(self, mock_load_config_file):
        self.mock_load_config_file = mock_load_config_file
        self.test_video_info = {'raw_file_name': 'media_filename', 'duration': 'media_duration', 'name': 'media_name',
                                'language': 'media_language', 'source': 'media_source', 'source_url': 'media_url',
                                'gender': 'media_gender', 'source_website': 'media_website', 'license': 'test_license'}
        self.test_config = {'downloader': {
            "mode": "complete",
            "audio_id": None,
            "cleaned_duration": None,
            "num_of_speakers": None,
            "language": "config_language",
            "has_other_audio_signature": "False",
            "type": "audio",
            "source": "config_source",
            "experiment_use": "False",
            "utterances_files_list": None,
            "source_website": "config_website",
            "experiment_name": None,
            "mother_tongue": None,
            "age_group": None,
            "recorded_state": None,
            "recorded_district": None,
            "recorded_place": None,
            "recorded_date": None,
            "purpose": None,
            "speaker_gender": "config_gender",
            "speaker_name": None
        }}
        self.mock_load_config_file.return_value = self.test_config
        self.media_metadata = MediaMetadata()

    def test_init(self):
        self.assertEqual(self.mock_load_config_file.return_value['downloader'], self.media_metadata.config_json)

    def test_create_metadata(self):
        actual_metadata = self.media_metadata.create_metadata(self.test_video_info)

        self.assertEqual(self.test_video_info['raw_file_name'], actual_metadata['raw_file_name'])
        self.assertEqual(self.test_video_info['duration'], actual_metadata['duration'])
        self.assertEqual(self.test_video_info['raw_file_name'], actual_metadata['title'])
        self.assertEqual(self.test_config['downloader']['audio_id'], actual_metadata['audio_id'])
        self.assertEqual(self.test_config['downloader']['cleaned_duration'], actual_metadata['cleaned_duration'])
        self.assertEqual(self.test_config['downloader']['num_of_speakers'], actual_metadata['num_of_speakers'])
        self.assertEqual(self.test_config['downloader']['has_other_audio_signature'],
                         actual_metadata['has_other_audio_signature'])
        self.assertEqual(self.test_config['downloader']['type'], actual_metadata['type'])
        self.assertEqual(self.test_config['downloader']['experiment_use'], actual_metadata['experiment_use'])
        self.assertEqual(self.test_config['downloader']['utterances_files_list'],
                         actual_metadata['utterances_files_list'])
        self.assertEqual(self.test_video_info['source_url'], actual_metadata['source_url'])
        self.assertEqual(self.test_config['downloader']['experiment_name'], actual_metadata['experiment_name'])
        self.assertEqual(self.test_config['downloader']['mother_tongue'], actual_metadata['mother_tongue'])
        self.assertEqual(self.test_config['downloader']['age_group'], actual_metadata['age_group'])
        self.assertEqual(self.test_config['downloader']['recorded_state'], actual_metadata['recorded_state'])
        self.assertEqual(self.test_config['downloader']['recorded_district'], actual_metadata['recorded_district'])
        self.assertEqual(self.test_config['downloader']['recorded_place'], actual_metadata['recorded_place'])
        self.assertEqual(self.test_config['downloader']['recorded_date'], actual_metadata['recorded_date'])
        self.assertEqual(self.test_config['downloader']['purpose'], actual_metadata['purpose'])

    @patch('data_acquisition_framework.metadata.metadata.load_config_file')
    def test_create_metadata_speaker_name_from_config(self, mock_load_config_file):
        self.test_config['downloader']["speaker_name"] = "test_speaker"
        mock_load_config_file.return_value = self.test_config
        media_metadata = MediaMetadata()

        actual_metadata = media_metadata.create_metadata(self.test_video_info)

        self.assertEqual(self.test_config['downloader']['speaker_name'], actual_metadata['speaker_name'])
        self.test_config['downloader']["speaker_name"] = None

    def test_create_metadata_speaker_name_from_media_info(self):
        actual_metadata = self.media_metadata.create_metadata(self.test_video_info)

        self.assertEqual(self.test_video_info['name'], actual_metadata['speaker_name'])

    def test_create_metadata_language_from_media_info(self):
        actual_metadata = self.media_metadata.create_metadata(self.test_video_info)

        self.assertEqual(self.test_video_info['language'], actual_metadata['language'])

    def test_create_metadata_language_from_config(self):
        del self.test_video_info['language']

        actual_metadata = self.media_metadata.create_metadata(self.test_video_info)

        self.assertEqual(self.test_config['downloader']['language'], actual_metadata['language'])

    def test_create_metadata_source_from_media_info(self):
        actual_metadata = self.media_metadata.create_metadata(self.test_video_info)

        self.assertEqual(self.test_video_info['source'], actual_metadata['source'])

    def test_create_metadata_source_from_config(self):
        del self.test_video_info['source']

        actual_metadata = self.media_metadata.create_metadata(self.test_video_info)

        self.assertEqual(self.test_config['downloader']['source'], actual_metadata['source'])

    def test_create_metadata_gender_from_media_info(self):
        self.test_config['downloader']['speaker_gender'] = None

        actual_metadata = self.media_metadata.create_metadata(self.test_video_info)

        self.assertEqual(self.test_video_info['gender'], actual_metadata['speaker_gender'])

    @patch('data_acquisition_framework.metadata.metadata.load_config_file')
    def test_create_metadata_gender_from_config(self, mock_load_config_file):
        self.test_config['downloader']["speaker_gender"] = "test_gender"
        mock_load_config_file.return_value = self.test_config
        media_metadata = MediaMetadata()

        actual_metadata = media_metadata.create_metadata(self.test_video_info)

        self.assertEqual(self.test_config['downloader']['speaker_gender'], actual_metadata['speaker_gender'])

    def test_create_metadata_source_website_from_media_info(self):
        actual_metadata = self.media_metadata.create_metadata(self.test_video_info)

        self.assertEqual(self.test_video_info['source_website'], actual_metadata['source_website'])

    def test_create_metadata_source_website_from_config(self):
        del self.test_video_info['source_website']

        actual_metadata = self.media_metadata.create_metadata(self.test_video_info)

        self.assertEqual(self.test_config['downloader']['source_website'], actual_metadata['source_website'])

    def test_create_metadata_license_in_media_info(self):
        actual_metadata = self.media_metadata.create_metadata(self.test_video_info)

        self.assertEqual(self.test_video_info['license'], actual_metadata['license'])

    def test_create_metadata_license_not_in_media_info(self):
        del self.test_video_info['license']

        actual_metadata = self.media_metadata.create_metadata(self.test_video_info)

        self.assertEqual('', actual_metadata['license'])
