<h1 align="center">X_405086 The Social Web (Project Group 3)</h1>

<p align="center">
    <img src="_documents/vu_logo.png" width="300" align="center">
</p>

<p align="center">
    <i>
        <b>
        Group Members (even contribution) : <br>
        Shuxin He: 2633040 (hsx510@gmail.com), 
        S.B. Schelvis: 2651448 (s.b.schelvis@student.vu.nl), 
        Shiva Jairam: 2630837 (s.a.jairam@student.vu.nl), 
        Jordy Ravesteijn: 2635721 (j.n.ravesteijn@student.vu.nl), 
        Kai Zhang: 2666387 (mail@kai.sh)
        </b>
    </i>
</p>


## General Idea

The US elections in 2020 is a trending topic on Social Network Services (SNS) such as Twitter and Facebook. SNS have proven themselves to be a great influence on society sparking controversy in for example the 2016 US elections. This gives rise to the following question: To what degree can data mining of social network services predict the outcome of an US election? The idea is to mine data from Twitter and Facebook related to the USA elections in 2016 surrounding the republican candidate "Dondald Trump" and democratic candidate "Hillary Clinton". The data collected can be compared to data of the real US elections outcomes of 2016. The gathered information and analysis of the data can present great insight in actual SNS influence on world politics.

Research question: To what degree can data mining of social network services predict the outcome of an US election?

## Data process pip-line
<p align="center">
    <img src="_documents/process_pipline.png" width="900" align="center">
</p>

## Project Information Visualization Demo
Our final presentation video is hosted on YouTube due to size restrictions and easy accessibility: https://youtu.be/WnoiCArH494

<p align="center">
    <img src="_documents/Screen-Caputre.png.png" width="900" align="center">
</p>

## Run project
1. Set default Java Version as Java 1.8
2. Install related python requirements
    ```shell
    pip3 install -r requirements.txt
    ```
3. Run the Flask's `app.py`
    ```shell
    flask app.py
    ```
## Potential problems
1. If you see `major 55 error`, it means you did not get right Java Configuration, spark only accept java 1.8, please check following link to config right Java version. https://stackoverflow.com/questions/21964709/how-to-set-or-change-the-default-java-jdk-version-on-os-x
2. It need 10 seconds to make server start and generate new word cloud.

