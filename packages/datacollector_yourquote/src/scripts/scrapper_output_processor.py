import pandas as pd
import re
data=pd.read_csv("yourquote.csv")
video_title=data["video-name"]
video_links=data["video-name-href"]
hindi_titles=[]
hindi_links=[]
hindi_speaker=[]
c=0;count=0
for i in range(len(video_title)):
    if re.compile(r'(hindi)', flags=re.IGNORECASE).search(video_title[i]):
        if not re.compile(r'(english)', flags=re.IGNORECASE).search(video_title[i]):
            count+=1
            hindi_titles.append("yourtalk_video_"+str(count))
            hindi_links.append(video_links[i])
            try:
                title=hindi_titles[i].split(" ")
                hindi_speaker.append("yourtalk_"+(" ".join(title[title.index("by")+1:title.index("by")+3]).replace("|","")).strip().replace(" ","_"))
            except:
                c+=1
                hindi_speaker.append("yourtalk_speaker_"+str(c))
df=pd.DataFrame({"hindi_video_title":hindi_titles,"hind_video_links":hindi_links,"speaker_names":hindi_speaker})
df.to_csv("yourquote_processed.csv",index=False)