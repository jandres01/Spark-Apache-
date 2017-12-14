Written by: Robbie Andres

# Project Synopsis

This repository is for my CSCI-3395 Big Data class in Trinity University. We'll be analyzing 2 files that are around 1mb size and 1 dataset that is at least 100MB.

# Links to the datasets!

pathway to Shark Attacks dataset~/jandres/CSCI3395/data/sharkAttacks.csv

Collected the Shark Attacks Worldwide dataset from https://www.statcrunch.com/app/index.php?dataid=2188687

pathway to Shark Attacks dataset~/jandres/CSCI3395/data/mlbSalaries.csv

Collected the MLB player salaries from https://www.statcrunch.com/app/index.php?dataid=2188689

The return-data.csv dataset was collected through gathering data the past semester. Special thanks to Dr. Stover for mostly collecting the data and helping clean it to be used as a dataset.

All the data in this folder is free for others to use and enjoy!

### SharkAttack dataset description

The website stated that this data comes from www.sharkattackfile.net. They have gathered all the recorded shark attacks worldwide even befoe 1800. The dataset includes information such as date, location, information on the individual who was attacked, details on the injuries sustained by the victim, and the species of the shark

### Shark Attacks questions that I would like to answer:

1. What countries does shark attacks most commonly occur?
2. What months are most common for a shark attack?
3. Are unprovoked shark attacks more fatal than provoked shark attacks?
4. What species of sharks are most commonly involved with shark attacks?

### Why I find this Shark Attacks and the questions interesting?

I am born in a tropical area surrounded by water. I always wondered if there is a trend among shark attacks and how to take actions to avoid it from occuring to anyone especially in the Philippines.  

### End Semester Shark Attack Analysis

My dataset is capable of answering all of my questions! To answer all the questions involves a simple groupby and aggregate count method. The third question will be comparing the resulting count of fatal attacks which won't be any trouble at all.

### MLB Dataset from 1985-2015

The mblSalaries.csv file contains all the salaries from every MLB player from 1985 to 2015. It has also recorded the team the player had played for, city of the team, league involved in and a unique player id.  

The player ID is the first 5 letters from the last name, followed by the first two letters from the first name, followed by a number in case of duplicate names. For example, bondsba01 stands for Barry Bonds with "01" because he's the first with the "bondsba" name ID. 

### Questions that I would like to answer using MLB dataset

1. Who had the longest career during this time period?
2. Do players who leave teams always experience a higher salary pay?
3. How many players stayed with the same team for their entire season?
4. Who had the greatest growth in salary?
5. Who had the longest streak of highest paid MBL athlete?

### Why I find MLB dataset & these questions interesting?

I have a lot of friends who know a lot of details about the MLB. I have never been interested nor have I played baseball. However, I want to relate more with them and hope by answering these questions I will be able to join their conversations when talking about MLB.

### End Semester MLB Data Analysis

The questions I asked in this dataset are trickier to solve but now I am capable of solving these questions thanks to Big Data. Question #2 will be difficult to solve for I would have to create a new column of previous salary and see that if a player changed his team did his prior salary grow. Also, measuring player growth in salary will be interesting to solve for I would have to create a new column as well and get the max value of growth column. 

### return-data.csv Historical stock value in the past century.

For my independent study last semester we have gathered the average monthly closing price, stock return, stock volume, sales, and key financial ratios (profit margin, p/e, etc) in order to predict the next stock return for the following months.

We have gathered data since the early 1900s with over 100 companies to choose from.

### Questions that I would like answered?

1. Do stocks with low prices have consistenly high financial ratios?
2. Are there any common trends that help predict if a company will experience financial problems?
3. If sales continue to grow does the stock price of the company grow as well?
4. What companies have experienced the highest stock price growth during this time period?

### Why Am I Interested in Stocks & these questions?

I am on tracks to graduate the spring of 2018 with a degree in Finance and Computer Science as a second degree major. I hope after graduation I will be able to work as a software developer for a financial firm in order to grow my knowledge in both finance and computer science. 

### End Semester Stock Data Analysis

It is not feasible to predict the next stock return for the next month because of too much unpredictability. Too much variables are involved in this analysis and if it were easy to do then everyone would have done this by now. I would have to consider the different models I would like to use for analyzing stocks and see how well my accuracy of the Network performs.
