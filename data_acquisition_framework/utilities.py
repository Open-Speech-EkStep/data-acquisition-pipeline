from moviepy import editor
from tinytag import TinyTag

from data_acquisition_framework.configs.paths import download_path


def get_mp3_duration_in_seconds(file):
    tag = TinyTag.get(file)
    return round(tag.duration, 3)


def get_license_info(license_urls):
    for url in license_urls:
        if "creativecommons" in url:
            return "Creative Commons"
    return ', '.join(license_urls)


def get_file_format(file):
    file_format = file.split('.')[-1]
    return file_format


def __get_duration_in_seconds(file):
    file_format = get_file_format(file)
    if file_format == 'mp4':
        video = editor.VideoFileClip(file)
        duration_in_seconds = int(video.duration)
    else:
        duration_in_seconds = get_mp3_duration_in_seconds(file)
    return duration_in_seconds


def __get_duration_in_minutes(duration_in_seconds):
    return round(duration_in_seconds / 60, 3)


def get_media_info(file, source, language, source_url, license_urls, media_url):
    duration_in_seconds = __get_duration_in_seconds(file)
    media_info = {'duration': __get_duration_in_minutes(duration_in_seconds),
                  'raw_file_name': file.replace(download_path, ""),
                  'name': None, 'gender': None,
                  'source_url': media_url,
                  'license': get_license_info(license_urls),
                  "source": source,
                  "language": language,
                  'source_website': source_url}
    return media_info, duration_in_seconds


def is_url_start_with_cc(url):
    creative_common_url_patterns = ["https://creativecommons.org/publicdomain/mark",
                                    "https://creativecommons.org/publicdomain/zero",
                                    "https://creativecommons.org/licenses/by"]
    return any(cc_pattern in url for cc_pattern in creative_common_url_patterns)


def check_for_cc_in_urls(urls): # not tested yet
    license_urls = set()
    for url in urls:
        url = url.rstrip().lstrip()
        if is_url_start_with_cc(url):
            license_urls.add(url)
    return license_urls


def is_license_terms_in_text(text):
    license_terms = ["terms", "license", "copyright",
                     "usage policy", "conditions",
                     "website policies", "website policy"]
    return any(license_term in text for license_term in license_terms)


def extract_license_urls(urls, all_a_tags, response):  # not tested yet
    license_urls = check_for_cc_in_urls(urls)
    if len(license_urls) == 0:
        for a_tag in all_a_tags:
            texts = a_tag.xpath('text()').extract()
            for text in texts:
                text = text.lower()
                if is_license_terms_in_text(text):
                    for link in a_tag.xpath('@href').extract():
                        license_urls.add(response.urljoin(link))
    return list(license_urls)


def is_unwanted_words_present(words_to_ignore, url):
    for word in words_to_ignore:
        if word in url.lower():
            return True
    return False


def is_unwanted_extension_present(extensions_to_ignore, url):
    for extension in extensions_to_ignore:
        if url.lower().endswith(extension):
            return True
    return False


def is_extension_present(extensions_to_include, url):
    for extension in extensions_to_include:
        if url.lower().endswith(extension.lower()):
            return True
    return False


def sanitize(word):
    return word.rstrip().lstrip().lower()


def is_unwanted_wiki(language_code, url):
    url = sanitize(url)
    if "wikipedia.org" in url or "wikimedia.org" in url:
        url = url.replace("https://", "").replace("http://", "")
        if url.startswith("en") or url.startswith(language_code) or url.startswith("wiki"):
            return False
        else:
            return True
    return False


def write(filename, content):
    with open(filename, 'a') as f:
        f.write(content + "\n")


def get_meta_filename(file):
    file_format = file.split('.')[-1]
    meta_file_name = file.replace(file_format, "csv")
    return meta_file_name
