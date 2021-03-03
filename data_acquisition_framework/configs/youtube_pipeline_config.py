from enum import Enum


class YoutubeService(Enum):
    YOUTUBE_DL = "YOUTUBE_DL",
    YOUTUBE_API = "YOUTUBE_API"


mode = 'channel'  # [channel,file]
only_creative_commons = True

# Common configurations
source_name = 'tamil_test'  # Scraped Data file name(CSV)
batch_num = 5  # keep batch small on free tier
youtube_service_to_use = YoutubeService.YOUTUBE_DL

# "https://www.youtube.com/channel/UC2XEzs5R1mn2wTKgtjuMxiQ": "sadhgurukannada_non_cc",
# Channel mode configurations
channel_url_dict = {
    # "https://www.youtube.com/channel/UC6Io5wojdZaMKOSMcughkvg": "GHOST",
    # "https://www.youtube.com/channel/UCbdMik2cV8pea1jcWdX_CYA": "Doordarshan Chandana",
    # "https://www.youtube.com/channel/UCYOv0QZr2B70Rkx_ZqIA84w": "NEWS ON AIR OFFICIAL"
    # "https://www.youtube.com/channel/UCGuxl6IDLwfVadPtWQzVE_g": "ALL INDIA RADIO BANGALORE"
    "https://www.youtube.com/channel/UC0HLXWlZV6RV0mkDdpUo73w": "Prakruthi_N__Banwasis_English_Classes",
    "https://www.youtube.com/channel/UC1NF71EwP41VdjAU1iXdLkw": "Narendra_Modi",
    "https://www.youtube.com/channel/UC1g2N64aEffX3YQ8AG_lmvw": "Prime_News24",
    "https://www.youtube.com/channel/UC1ghFxXf1yMzIHUiCx718VQ": "Kannada_whats_up_video_comedy_show",
    "https://www.youtube.com/channel/UC4KypMnzpSyu9cazArIw7dA": "SRI_SUDDI_KANNADA_NEWS",
    "https://www.youtube.com/channel/UC8fbOoRL6hf66kwxs-O3WeQ": "SRI_TV_SIRA",
    "https://www.youtube.com/channel/UCAL_0BpBVgOW186lV-VpVlg": "Kannada_NewsI_ಕನ್ನಡ_ನ್ಯೂಸ್",
    "https://www.youtube.com/channel/UCFEPbJT78sJ-wi2xnZ71c0Q": "MarshMilanVines",
    "https://www.youtube.com/channel/UCG30gCpmEgzdiBtp8kVfliQ": "CLASSIC_EDUCATION",
    "https://www.youtube.com/channel/UCJ7KmzMc_OwrjVeKUjOWngQ": "Discover_Islam_Education_Trust_-_DIET",
    "https://www.youtube.com/channel/UCKKSLsoaTr2ZDmEv81vJLSw": "News_101",
    "https://www.youtube.com/channel/UCKc5dDv8vjQvFagFcfUXN1w": "Ayaz_Mughal",
    "https://www.youtube.com/channel/UCL300NiMjrw7_6VJIpRAUzQ": "SHASHIDHAR_JAKKAMMANAVAR",
    "https://www.youtube.com/channel/UCLJhqFiwQ6YXWQc15L68Ojg": "Government_Jobs",
    "https://www.youtube.com/channel/UCN_fxPMt8rVI2XEbzL3NtCw": "Indian_TV_Karnataka_-_ಇಂಡಿಯನ್_ಟಿವಿ_ಕರ್ನಾಟಕ",
    "https://www.youtube.com/channel/UCNiBCI_WpQ_cnXbjKkfSZIg": "sachi_tv",
    "https://www.youtube.com/channel/UCO878KFQWJCpFM9bPJWhAwg": "Troll_mavandru",
    "https://www.youtube.com/channel/UCPMDhcBogBPsrGQWf5qnArg": "Tech_in_Kannada",
    "https://www.youtube.com/channel/UCR-b_zfZcPVeTnpa-2N-FOQ": "Thulasi_prasad_Sh",
    "https://www.youtube.com/channel/UCVECe1XIrHcoI0ZxWPhcyxA": "CRCKET_NEWS",
    "https://www.youtube.com/channel/UCVeOHts1owNULriHSMGVJAw": "Ks_Kannada_",
    "https://www.youtube.com/channel/UCVuc001PT0KCo-bwusirhpA": "Kannada_JOB_News_",
    "https://www.youtube.com/channel/UCWJccPQPQ00eEmZf5D5qSAg": "Kannada_DriveSpark",
    "https://www.youtube.com/channel/UCWz3LfazJNepr2aPUR4KEcg": "UK_KANNADA_NEWS",
    "https://www.youtube.com/channel/UCXxUayhQHz_NC6lUCoAmJjg": "Surya_Murugan",
    "https://www.youtube.com/channel/UC_zToADFDNqxIDoWCrmZVaA": "Wealth_Info",
    "https://www.youtube.com/channel/UCbtj0GbzxxlzIFTMrqtI4MQ": "Inside_News_Kannada",
    "https://www.youtube.com/channel/UCbtj0GbzxxlzIFTMrqtI4MQ": "Maran_News",
    "https://www.youtube.com/channel/UCdbcS38CHCKVWSYtBBD0SRQ": "Vidyarthi_Mitra",
    "https://www.youtube.com/channel/UCeKa_t70ZO4YE78n6fUJs6A": "Sampoorna_News__ಸಂಪೂರ್ಣ_ನ್ಯೂಸ್__ಕನ್ನಡ",
    "https://www.youtube.com/channel/UChbzmYDb42Lt9BiW5z1xgTA": "News_India_Kannada",
    "https://www.youtube.com/channel/UCjGykUoVi-NtLmzIbhQytTw": "Kannada_News_Now",
    "https://www.youtube.com/channel/UCqgts67ZwDd-gAasDQbZrnQ": "Jhankar_Music",
    "https://www.youtube.com/channel/UCw2i89AQaWRTiqKo5F2DXRA": "Bharath_Jain",
    "https://www.youtube.com/channel/UCw9hSaHhi-jciTJ_SOIhhXg": "ITTEHAD_NEWS",
    "https://www.youtube.com/channel/UCx7ZVJjdmG1yqocLH7sLQYw": "WWE_News_Kannada",
    "https://www.youtube.com/channel/UCxmhl3MfjqVAaJsXp_2jGVQ": "AB5_News_-_ಕನ್ನಡ",
    "https://www.youtube.com/channel/UCziTM2UndVHq50FtgJ7NSNA": "KANNADA_TECH_-_ಕನ್ನಡ_ಟೆಕ್"
}

# File Mode configurations
file_speaker_gender_column = 'speaker_gender'
file_speaker_name_column = "speaker_name"
file_url_name_column = "video_url"
license_column = "license"
