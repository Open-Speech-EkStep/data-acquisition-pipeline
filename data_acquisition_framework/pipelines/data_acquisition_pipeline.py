import abc


class DataAcquisitionPipeline(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def create_download_batch(self, item):
        pass

    @abc.abstractmethod
    def download_files(self, item, batch_list):
        pass

    @abc.abstractmethod
    def extract_metadata(self, item, media_file_name, url):
        pass

    @abc.abstractmethod
    def process_item(self, item, spider):
        pass
