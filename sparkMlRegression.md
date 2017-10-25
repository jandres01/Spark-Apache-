## Written by Jose Roberto Andres

#### In Class Question #1

|summary|          GENHLTH|
| -- | -- |
|  count|           486303|
|   mean|5.473830101808955|
| stddev|3.447015277360491|
|    min|             -1.0|
|    max|              9.0|

|summary|         PHYSHLTH|
| -- | -- |
|  count|           486303|
|   mean| 65.5723818277905|
| stddev|31.16934023520132|
|    min|              0.0|
|    max|             99.0|

|summary|          MENTHLTH|
| -- | -- |
|  count|            486303|
|   mean|30.810548156190688|
| stddev|32.125000004590426|
|    min|               0.0|
|    max|              99.0|

|summary|          POORHLTH|
| -- | -- |
|  count|            486303|
|   mean|29.994067484675192|
| stddev| 36.05172810537815|
|    min|               1.0|
|    max|              99.0

|summary|           EXERANY2|
| -- | -- |
|  count|             486303|
|   mean|0.12180471845742263|
| stddev| 0.8013351687687944|
|    min|               -1.0|
|    max|                9.0|

|summary|         SLEPTIM1|
| -- | -- |
|  count|           486303|
|   mean|68.85272145144077|
| stddev|16.74114625597331|
|    min|             -1.0|
|    max|             99.0|

#### Out of Class Problems
1. With the 275 columns of data we decided to create a correlation matrix of all the columns with one another. Then we would create a list with 4 columns that have the highest positive correlation value to the compared column. Afterwards, we would test all the possible combinations and pass them in to the linear regression for the column being compared.

I have found that GENHTLH & PHYSHLTH are strongly correlated to one another. Having the same top 3 columns correlated to them which are: Arthritis (HAVARTH3), ASTHMA (_ASTHMS1), chronic obstructive pulmonary disease, emphysema or chronic bronchitis (CHCCOPD1). 

GenHTLTH ListBuffer(PHYSHLTH, CHCCOPD1, HAVARTH3, _ASTHMS1)

PhysHTLTH ListBuffer(GENHLTH, CHCCOPD1, HAVARTH3, _ASTHMS1)

Describe Gen
|summary|        prediction|
| -- | -- |
|  count|            486303|
|   mean| 5.473830101809752|
| stddev|0.7183058247918199|
|    min|0.4171342707790342|
|    max|11.174857134696518|

Describe PHYS
|summary|        prediction|
| -- | -- |
|  count|            486303|
|   mean| 65.57238182777938|
| stddev| 6.467307073893003|
|    min|24.603521204233616|
|    max|  172.619090783844|

I have also found that there is a strong correlation between Mental Health & poor phyical or mental health (POORHLTH). Mental health has strong correlations to Veterans (VETERAN3), Adults with good health (_RFHLTH), and computed physical health (_PHYS14D). POORHTLTH also had strong correlations to permanent teeth removed (RMVTETH3) as well as both _RFHLTH, _PHYS14D.

MentHTLTH ListBuffer(POORHLTH, VETERAN3, _RFHLTH, _PHYS14D)

PoorHTLTH ListBuffer(MENTHLTH, RMVTETH3, _RFHLTH, _PHYS14D)

Describe MENT
|summary|        prediction|
| -- | -- |
|  count|            486303|
|   mean| 30.81054815619337|
| stddev|15.074344928993826|
|    min| 23.02989033804414|
|    max|116.43343212097741|

Describe POOR
|summary|        prediction|
| -- | -- |
|  count|            486303|
|   mean|  29.9940674846786|
| stddev|12.068862338203607|
|    min|21.926986511629785|
|    max|123.57409595961506|




 

