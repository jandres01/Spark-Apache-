## Written by Robbie Andres

Behavior of the Stock Market with Information Uncertainty

The stock market is filled with so much uncertainty. There are a large number of possibilities for a change in variable that it has become difficult to make future predictions. For instance, analyzing the reason profit margin decreased could be caused by a decrease in net income or even an increase in sales. 

For the final project of Big Data, I wanted to see if I am capable of finding new information with my financial data. There are 2 questions I will be focusing on for this project. Firstly, I wanted to know what financial data are highly correlated during different status of the economy? Secondly, I wanted to see how companies performed during the past 3 economic crisis. Therefore, the 2 machine learning techniques I would use are a correlation matrix and a linear regression. 

I collected my data set from Wharton University, who gathered publicly traded companies daily financial information. For this project, I decided it would be much more beneficial to use monthly information then daily data. Therefore, I wrote a script to average all the values for each column for each company to create a data set with a size of 1.2G. My data set has information on 2,205 publicly traded companies with information since 1907. My data set also had 99 columns of financial data and I decided to use a subset: price, return, sales, assets, profit_margin, roe, price_to_earnings, price_to_book, price_to_sales, next_return. I also created subgroups of data filtered by date: oil crisis (1971-1975), tech Bubble (1997-2001), financial crisis (2006-2010), normal economy (1981-1991)

To begin the project I wrote a correlation matrix of the entire data set. 

![Alt Text]("/users/jandres/Desktop/BGPics/Whole\ economy.png")

I discovered that sales, price and assets have positive correlations with one another. Surprisingly, sales and assets even have a 54.29% relationship. However, I began wondering how the relationships reacted during a financial crisis and good economic times. 

Correlation during economic crisis

![Alt Text]("/users/jandres/Desktop/BGPics/bad\ economy.png")

Correlation during normal economy

![Alt Text]("/users/jandres/Desktop/BGPics/Good\ Economy.png")

During an economic crisis, we would get similar results to the correlation of data for the entire data set. However, during a normal economy we could find positive correlations with next_return and return on equity. What this tells us is that depending on the status of the economy we would find different relationships between financial data. If the economy is doing well, we may have a higher possibility of predicting future data.

For my second machine learning technique, I decided to create a linear regression for predicting return of equity for whole data set and its subsets. I passed in 2 columns with highest positive correlation, which are price and sales. The S&P500 has a current ROE average of 15.68% as of Dec 12, 2017. Therefore, green points would have an ROE >= 25% and red points would have an ROE <=10%.

![Alt Text]("/users/jandres/Desktop/BGPics/ROE\ whole\ data.png")
![Alt Text]("/users/jandres/Desktop/BGPics/ROE\ good\ data.png")
![Alt Text]("/users/jandres/Desktop/BGPics/ROE\ oil\ Crisis.png")
![Alt Text]("/users/jandres/Desktop/BGPics/ROE\ tech\ bubble.png")
![Alt Text]("/users/jandres/Desktop/BGPics/ROE\ financial\ crisis.png")

ROE is calculated by Net Income / Shareholder Equity. It is a measure of a company’s profitability that calculates profit generated for each dollar of shareholder equity. Having an ROE of 50% would mean you generate 0.50 cents for each dollar of shareholder equity invested into the company. I was shocked to realize that during an economic crisis ROE would increase. How is this possible? Is Net Income actually increasing and is shareholder equity decreasing? I created another linear regression to predict profit margins = net income/ sales to see the reaction of Net Income during a crisis.

![Alt Text]("/users/jandres/Desktop/BGPics/Full\ Data.png")
![Alt Text]("/users/jandres/Desktop/BGPics/Good\ Data")
![Alt Text]("/users/jandres/Desktop/BGPics/Oil\ Crisis.png")
![Alt Text]("/users/jandres/Desktop/BGPics/Tech\ Bubble.png")
![Alt Text]("/users/jandres/Desktop/BGPics/Financial\ Crisis.png")


What is surprising is that we can see that during the Oil Crisis net Income was impacted the most probably because expenses increased for all the companies. During the Tech Bubble and Financial Crisis Net Income was beginning to match Sales therefore companies were decreasing their costs for profit margins to be similar to regular economy. 







