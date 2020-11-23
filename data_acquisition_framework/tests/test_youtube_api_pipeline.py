from unittest import TestCase
from unittest.mock import patch, Mock

from data_acquisition_framework.pipelines.youtube_api_pipeline import YoutubeApiPipeline

pipeline_path = 'data_acquisition_framework.pipelines.youtube_api_pipeline'


class TestYoutubeApiPipeline(TestCase):
    @patch(pipeline_path + '.MediaMetadata')
    @patch(pipeline_path + '.YoutubeUtil')
    @patch(pipeline_path + '.StorageUtil')
    def setUp(self, mock_storage_util, mock_youtube_util, mock_media_metadata):
        self.mock_storage_util = mock_storage_util
        self.mock_youtube_util = mock_youtube_util
        self.mock_media_metadata = mock_media_metadata
        self.youtube_api_pipeline = YoutubeApiPipeline()
        self.test_item = {'channel_name': 'test_channel', 'filename': 'testid__test_channel', 'channel_id': 'testid',
                          'filemode_data': ''}

    def create_patch(self, name):
        patcher = patch(name)
        thing = patcher.start()
        self.addCleanup(patcher.stop)
        return thing

    def test_init(self):
        self.assertEqual(self.mock_storage_util.return_value, self.youtube_api_pipeline.storage_util)
        self.assertEqual(self.mock_youtube_util.return_value, self.youtube_api_pipeline.youtube_util)
        self.assertEqual(self.mock_media_metadata.return_value, self.youtube_api_pipeline.metadata_creator)
        self.assertEqual(0, self.youtube_api_pipeline.batch_count)

    @patch(pipeline_path + '.get_video_batch')
    def test_create_download_batch(self, mock_get_video_batch):
        self.youtube_api_pipeline.create_download_batch(self.test_item)

        mock_get_video_batch.assert_called_once_with(self.test_item['channel_name'], self.test_item['filename'])

    def test_download_files(self):
        test_batch_list = ['testid1, testid2, testid3']

        self.youtube_api_pipeline.download_files(self.test_item, test_batch_list)

        self.mock_youtube_util.return_value.download_files.assert_called_once_with(self.test_item['channel_name'],
                                                                                   self.test_item['filename'],
                                                                                   test_batch_list)

    @patch(pipeline_path + '.pd')
    @patch(pipeline_path + '.get_meta_filename')
    def test_extract_metadata(self, mock_get_meta_filename, mock_pd):
        test_filename = 'test.mp4'
        self.youtube_api_pipeline.extract_metadata(self.test_item, test_filename)
        mock_get_meta_filename.assert_called_once_with(test_filename)
        mock_video_info = self.mock_youtube_util.return_value.get_video_info
        mock_video_info.assert_called_once_with(test_filename,
                                                self.test_item['channel_name'],
                                                self.test_item['filemode_data'],
                                                self.test_item['channel_id'])

        mock_metadata = self.mock_media_metadata.return_value.create_metadata

        mock_metadata.assert_called_once_with(mock_video_info.return_value)
        mock_meta_df = mock_pd.DataFrame
        mock_meta_df.assert_called_once_with([mock_metadata.return_value])
        mock_meta_df.return_value.to_csv.assert_called_once_with(mock_get_meta_filename.return_value, index=False)

    def test_process_item(self):  # Logging is not tested
        test_spider = Mock()
        self.assertEqual(0, self.youtube_api_pipeline.batch_count)
        mock_batch_download = self.create_patch(pipeline_path + '.YoutubeApiPipeline.create_download_batch')

        self.youtube_api_pipeline.process_item(self.test_item, test_spider)

        self.mock_storage_util.return_value.retrieve_archive_from_bucket.assert_called_once_with(
            self.test_item['channel_name'])
        mock_batch_download.assert_called_once_with(self.test_item)

    def test_video_batch_exists_for_true(self):
        test_batch_list = ['testid1, testid2, testid3']

        self.assertTrue(self.youtube_api_pipeline.video_batch_exists(test_batch_list))

    def test_video_batch_exists_for_false(self):
        test_batch_list = []

        self.assertFalse(self.youtube_api_pipeline.video_batch_exists(test_batch_list))

    def test_batch_download(self):  # More tests for try except
        mock_create_download_batch = self.create_patch(
            pipeline_path + '.YoutubeApiPipeline.create_download_batch')
        mock_download_files = self.create_patch(
            pipeline_path + '.YoutubeApiPipeline.download_files')
        mock_upload_files_to_storage = self.create_patch(
            pipeline_path + '.YoutubeApiPipeline.upload_files_to_storage')
        mock_video_batch_exists = self.create_patch(
            pipeline_path + '.YoutubeApiPipeline.video_batch_exists')
        mock_video_batch_exists.side_effect = [True, True, False]
        batch_list = mock_create_download_batch.return_value

        self.youtube_api_pipeline.batch_download(self.test_item)

        self.assertEqual(3, mock_create_download_batch.call_count)
        mock_create_download_batch.assert_called_with(self.test_item)
        self.assertEqual(2, mock_download_files.call_count)
        mock_download_files.assert_any_call(self.test_item, batch_list)
        self.assertEqual(2, mock_upload_files_to_storage.call_count)

    def test_upload_files_to_storage(self):
        video_file_one = 'a.mp4'
        video_file_two = 'b.mp4'
        self.mock_storage_util.return_value.get_media_paths.side_effect = [[video_file_one, video_file_two]]
        mock_extract_metadata = self.create_patch(
            pipeline_path + '.YoutubeApiPipeline.extract_metadata')

        self.youtube_api_pipeline.upload_files_to_storage(self.test_item)

        self.mock_storage_util.return_value.get_media_paths.assert_called_once()
        self.assertEqual(2, mock_extract_metadata.call_count)
        mock_extract_metadata.assert_any_call(self.test_item, video_file_one)
        mock_extract_metadata.assert_any_call(self.test_item, video_file_two)
        self.assertEqual(2, self.mock_storage_util.return_value.upload_media_and_metadata_to_bucket.call_count)
        self.mock_storage_util.return_value.upload_media_and_metadata_to_bucket.assert_any_call(
            self.test_item['channel_name'], video_file_one)
        self.mock_storage_util.return_value.upload_media_and_metadata_to_bucket.assert_any_call(
            self.test_item['channel_name'], video_file_two)
        self.assertEqual(2, self.youtube_api_pipeline.batch_count)

    def test_upload_files_to_storage_with_no_media_paths(self):
        self.mock_storage_util.return_value.get_media_paths.side_effect = [[]]
        mock_extract_metadata = self.create_patch(
            pipeline_path + '.YoutubeApiPipeline.extract_metadata')

        self.youtube_api_pipeline.upload_files_to_storage(self.test_item)

        self.mock_storage_util.return_value.get_media_paths.assert_called_once()
        mock_extract_metadata.assert_not_called()
        self.mock_storage_util.return_value.upload_media_and_metadata_to_bucket.assert_not_called()
        self.assertEqual(0, self.youtube_api_pipeline.batch_count)
