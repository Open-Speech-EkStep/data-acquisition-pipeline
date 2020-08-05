import abc


class DataAcqusitionPipeline(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def scrape_links(self):
        pass

    @abc.abstractmethod
    def create_download_batch(self):
        pass

    @abc.abstractmethod
    def download_files(self):
        pass

    @abc.abstractmethod
    def extract_metadata(self,file):
        pass

    @abc.abstractmethod
    def upload_to_bucket(self,file):
        pass
