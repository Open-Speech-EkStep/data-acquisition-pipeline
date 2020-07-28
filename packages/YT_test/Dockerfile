FROM scrapinghub/scrapinghub-stack-scrapy:1.3
ENV TERM xterm
ENV SCRAPY_SETTINGS_MODULE YT_test.settings
RUN apt-get install sox
RUN mkdir -p /app
WORKDIR /app
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
RUN python setup.py install

