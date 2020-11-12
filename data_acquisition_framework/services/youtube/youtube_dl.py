import logging
import os
import subprocess


class YoutubeDL:
    def __init__(self):
        self.youtube_call = "/app/python/bin/youtube-dl " if "scrapinghub" in os.path.abspath("~") else "youtube-dl "

    def get_videos(self, channel_url, match_title_string, reject_title_string):
        command = self.youtube_call + '{0} --flat-playlist --get-id --match-title "{1}" --reject-title "{2}"'.format(
            channel_url, match_title_string, reject_title_string)
        downloaded_output = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        video_list = downloaded_output.stdout.decode("utf-8").rstrip().lstrip().split("\n")
        return video_list

    def youtube_download(self, video_id, archive_path, download_path):
        command = self.youtube_call + '-f "best[ext=mp4][filesize<1024M]" -o "{2}%(duration)sfile-id%(id)s.%(ext)s" ' \
                                      '"https://www.youtube.com/watch?v={0}" --download-archive {1} --proxy "" ' \
                                      '--abort-on-error'.format(video_id, archive_path,
                                                                download_path)
        downloader_output = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return YoutubeDL.check_and_log_download_output(downloader_output), video_id

    @staticmethod
    def check_and_log_download_output(downloader_output):
        remove_video_flag = False
        if downloader_output.stderr:
            formatted_error = str(downloader_output.stderr.decode("utf-8"))
            check = False
            if not ("WARNING" in formatted_error):
                logging.error(formatted_error)
            if ": YouTube said: Unable to extract video data" in formatted_error:
                video_id = formatted_error.split(":")[1].strip()
                remove_video_flag = True
                check = True
                logging.info(str("Video I'd {0} removed from channel and won't be downloaded".format(video_id)))
            if "Did not get any data blocks" in formatted_error or "HTTP Error 404: Not Found" in formatted_error:
                remove_video_flag = True
                check = True
                logging.info(str("ERROR Handled"))
            if "HTTP Error 429" in formatted_error:
                logging.error("Too many Requests... \nAborting..... \nPlease Re-Deploy")
                exit()
            if len(formatted_error) > 5 and not check:
                remove_video_flag = True
                logging.info(str("ERROR Handled"))
        formatted_output = downloader_output.stdout.decode("utf-8").split("\n")
        for _ in formatted_output:
            logging.info(str(_))
        return remove_video_flag