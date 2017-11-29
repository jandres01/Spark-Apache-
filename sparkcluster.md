## Written by Jose Roberto Andres

#### In Class Questions

1.
 
|agglvl_code|  count|
| -- | -- |
|         73| 277932|
|         71|  51964|
|         70|  13084|
|         75|1100372|
|         78|3984876|
|         77|3396052|
|         72|  76460|
|         74| 406868|
|         76|2162440|

2. There are 9244 entries for Bexar County

3. 3 most common industry codes by number of records 

|industry_code|count|
| -- | -- |
|           10|76952|
|          102|54360|
|         1025|40588|

4. industries with largest total wages

|industry_code|             sum|
| -- | -- |
|       611110| 2.4432185309E11|
|       551114| 2.1938760563E11|
|       622110|2.05806016743E11|

#### Out of Class Questions

1. I selected the 3 columns: "avg_wkly_wage lq_qtrly_estabs oty_total_qtrly_wages_chg." The accuracy of the chosen columns for 2 clusters is 48.54%. Clearly showing that I am terrible with picking columns. I attempted to see the accuracy of the selected columns with 3 clusters. I got an accuracy of 33.75%. Showing that the model does not perform well with additional possible answers.

For fun I wanted to see how bad I was in selecting columns because I could not be that bad. After a couple tries I chose columns: "avg_wkly_wage, taxable_qtrly_wages month1_emplvl." With 2 clusters I got an accuracy of 32.26%. With 3 clusters I got an accuracy of 31.87%. Validating the fat I am poor at selecting columns and should have made a correlation matrix instead.

Looking back I wonder if I was suppose to average the numerous predictions for each county? My model is currently predicting each row for each country which may be the reason for my poor accuracy rates. Groupby and average selected columns could have possibly decreased my incorrect values.

2. My graph for 2 clusters looks greate in showing there were more counties that went republican. View image png file, "Presidential Voting 2 Clusters" for 2 clusters. However, the graph for 3 clusters is divided having an area be neutral. The screenshot with 3 clusters is called, "Presidential Voting 3 Clusters"


