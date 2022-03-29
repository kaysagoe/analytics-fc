#!/bin/bash
wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
echo "deb http://dl.google.com/linux/chrome/deb/ stable main" | tee -a /etc/apt/sources.list.d/google-chrome.list
apt-get update -qqy
apt-get -qqy install google-chrome-stable
CHROME_VERSION=$(google-chrome-stable --version)
CHROME_FULL_VERSION=${CHROME_VERSION%%.*}
CHROME_MAJOR_VERSION=${CHROME_FULL_VERSION//[!0-9]}
rm /etc/apt/sources.list.d/google-chrome.list
export CHROMEDRIVER_VERSION=`curl -s https://chromedriver.storage.googleapis.com/LATEST_RELEASE_${CHROME_MAJOR_VERSION%%.*}`
curl -L -O "https://chromedriver.storage.googleapis.com/${CHROMEDRIVER_VERSION}/chromedriver_linux64.zip"
unzip chromedriver_linux64.zip && chmod +x chromedriver && mv chromedriver /usr/local/bin
export CHROMEDRIVER_VERSION=`curl -s https://chromedriver.storage.googleapis.com/LATEST_RELEASE_${CHROME_MAJOR_VERSION%%.*}`
curl -L -O "https://chromedriver.storage.googleapis.com/${CHROMEDRIVER_VERSION}/chromedriver_linux64.zip"
unzip chromedriver_linux64.zip && chmod +x chromedriver && mv chromedriver /usr/local/bin
chromedriver -version