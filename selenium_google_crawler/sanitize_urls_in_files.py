urls = []
with open('urls.txt', 'r') as f:
    for url in f.readlines():
        if url not in urls:
            urls.append(url)

with open('urls.txt','w') as f:
    f.writelines(urls)

urls = []
with open('archive.txt', 'r') as f:
    for url in f.readlines():
        if url not in urls:
            urls.append(url)

with open('archive.txt','w') as f:
    f.writelines(urls)