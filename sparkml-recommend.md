## Written By Jose Roberto Andres

#### In Class Problems

1. Range of UserID
		
|summary|            userID|
| -- | -- |
|  count|          24053764|
|   mean|1322285.3422910443|
| stddev| 764577.9360816329|
|    min|                 6|
|    max|           2649429|

2. There are 470758 distinct userIDs
3. User 372233 gave 47 5 star rating
4. Movie with most user ratings

|movieID|count |
| -- | -- |
|571    |154832|

|movieID|yearRelease|title          |
| -- | -- | -- |
|571    |1999       |American Beauty|

5. Movie with most 5 star rating

|movieID|count|
| -- | -- |
|571    |57537|

|movieID|yearRelease|title          |
| -- | -- | -- |
|571    |1999       |American Beauty|

#### Out of Class Problems

1. 

Top 5 Movie Recommendations for Each User

|userID|recommendations                                                                          |
| -- | -- |
|6     |[[3895,4.5019045], [1753,4.3904824], [2393,4.329475], [2879,4.2975335], [1396,4.2811]]   |
|7     |[[2879,5.3455005], [3895,5.2819448], [3924,5.274149], [2393,5.208823], [3456,5.143163]]  |
|8     |[[1109,5.5133705], [1577,5.4881907], [2393,5.3438115], [15,5.2878485], [1675,5.205831]]  |
|10    |[[933,4.447444], [1396,4.4241633], [1109,4.4136653], [3115,4.4121475], [2393,4.39291]]   |
|25    |[[4245,5.2968025], [1672,5.2446985], [3924,5.2243223], [1243,5.218335], [2393,5.2036743]]|
|33    |[[3924,5.0554113], [1109,5.0433636], [1304,5.021993], [3902,5.008485], [3895,4.8681836]] |
|42    |[[1396,5.129181], [3895,5.072348], [1753,5.0255833], [3924,5.01778], [3115,5.006987]]    |
|59    |[[2393,5.4416246], [2879,5.4220424], [3895,5.263533], [4261,5.196968], [3338,5.1044507]] |
|79    |[[1109,5.2948966], [3924,4.951869], [3115,4.735372], [3973,4.720796], [1396,4.7029557]]  |
|83    |[[3247,5.7421575], [933,5.557428], [2062,5.462993], [1773,5.4527035], [2691,5.4422574]]  |
|87    |[[2879,5.2861395], [3636,5.2648234], [3770,5.2386036], [1468,5.169264], [4046,5.0931153]]|
|94    |[[3770,5.511499], [1468,5.22433], [3895,5.1601353], [2105,5.060089], [3524,4.987296]]    |
|97    |[[229,4.6874304], [3770,4.540376], [1433,4.498501], [4427,4.4321413], [2138,4.4102664]]  |
|116   |[[2393,4.8726916], [1644,4.8615017], [933,4.856268], [4427,4.841174], [498,4.8247013]]   |
|126   |[[1396,5.977957], [1109,5.8204427], [2393,5.7186766], [2635,5.6754227], [3288,5.6635733]]|
|130   |[[2879,5.515627], [3019,5.2373705], [1468,5.164134], [3636,5.1180835], [3456,5.002679]]  |
|131   |[[2879,3.980991], [3019,3.7567961], [3636,3.7451417], [3456,3.7243211], [2691,3.6627758]]|
|133   |[[3924,3.795977], [3362,3.3769426], [559,3.3341699], [49,3.3323421], [1758,3.2870271]]   |
|134   |[[1753,6.3891068], [1396,6.176893], [1577,6.1233773], [2879,6.0146666], [3895,5.991078]] |
|142   |[[3924,5.095054], [1784,4.537203], [3635,4.497063], [1109,4.4824123], [1879,4.474503]]   |

Top 10 Most Commonly Recommended Movies

|movieID|count|title                                        |
| -- | -- | -- |
|2879   |6910 |Boz Scaggs: Greatest Hits Live               |
|2393   |5029 |The Blues Brothers: Theatrical Cut           |
|3895   |3220 |The Man Who Never Was                        |
|1109   |3192 |My Fair Lady: Special Edition: Bonus Material|
|3924   |2911 |White Sun of the Desert                      |
|3019   |2874 |Warren Millers Journey                       |
|1396   |2789 |Kaleido Star                                 |
|1753   |2361 |Making Marines                               |
|3636   |2338 |The Work of Director Mark Romanek            |
|4028   |2230 |Russell Simmons Presents Def Poetry: Season 2|

2. Quality of Movie Recommendations

Root-mean-square error = 0.9086335055308742 or machine learning is giving +/- 0.9086335055308742 rating from what users gave in actual data

|userID|recommendations                                                                                                                                                                 |
| -- | -- |
|31983 |[[229,5.4692316], [1376,4.8818207], [1468,4.8678217], [3247,4.7507987], [3277,4.705214], [735,4.6611857], [4048,4.646327], [2138,4.641554], [1072,4.6085873], [3362,4.6043105]] |
|40383 |[[4347,3.6959908], [229,3.6504738], [498,3.6128922], [3674,3.5966163], [2138,3.586686], [2267,3.5729377], [1076,3.538644], [1335,3.5364108], [2034,3.5137665], [1446,3.4900377]]|
|40653 |[[2879,5.2227902], [2393,5.164133], [3115,5.1127667], [49,5.108111], [1139,5.077973], [3456,5.070047], [3033,5.0344515], [3016,5.0035534], [3866,4.9957523], [3001,4.980119]]   |
|50353 |[[4261,5.895413], [2106,5.827712], [1879,5.5727277], [3338,5.543174], [157,5.377261], [1997,5.286027], [933,5.2675223], [3524,5.2077546], [1857,5.1979246], [3661,5.19273]]     |
|64423 |[[2879,5.3727694], [3115,4.922749], [3636,4.860037], [2902,4.8550215], [3016,4.8485665], [3456,4.8143315], [4304,4.8066664], [498,4.790038], [1628,4.77577], [2102,4.7319403]]  |
|73683 |[[2879,5.6400495], [3928,5.051015], [3456,5.0358477], [3057,5.0165596], [3033,5.001804], [498,4.997636], [3636,4.996243], [933,4.943178], [1628,4.9346933], [2728,4.9125805]]   |
|76143 |[[3277,5.6969147], [1040,5.246321], [3381,4.872288], [4318,4.823678], [1491,4.715993], [458,4.6775856], [4445,4.5796747], [2203,4.481743], [1877,4.4690995], [3288,4.461418]]   |
|77803 |[[2393,5.488808], [4041,5.3952475], [2526,5.37193], [1109,5.3093443], [3115,5.3084116], [861,5.2598677], [1761,5.2273855], [4188,5.2250576], [3895,5.2008977], [933,5.1595397]] |
|2659  |[[1109,6.6725955], [780,6.378631], [3674,6.32161], [1076,6.1282496], [3997,5.9774547], [3112,5.8149405], [2237,5.783207], [3973,5.7349677], [2325,5.7214904], [3902,5.684792]]  |
|28759 |[[2879,4.315861], [1705,4.1168156], [3172,4.0874343], [3430,4.0781283], [2406,4.062359], [3019,4.052337], [3115,4.042367], [1072,4.042151], [3290,4.031636], [3456,4.0033703]]  |
|29719 |[[229,5.1104302], [1109,4.964627], [3247,4.950647], [498,4.87753], [3518,4.871453], [458,4.869292], [3924,4.848422], [2203,4.8331714], [3958,4.8146105], [3456,4.806459]]       |
|32539 |[[4188,4.0737014], [933,4.050573], [1994,4.0374074], [270,4.019001], [1628,3.9368033], [2649,3.9303737], [522,3.9278545], [3425,3.8484273], [1688,3.8432786], [4008,3.7976654]] |
|34239 |[[1072,5.440395], [1705,5.3923073], [4347,5.3766136], [1376,5.3158774], [1977,5.275825], [2138,5.254167], [2833,5.2231197], [2803,5.218614], [3005,5.2110853], [4427,5.1848907]]|
|34759 |[[1714,5.4388876], [2270,5.225457], [2039,5.152713], [1903,4.995902], [3448,4.7213326], [2688,4.7012067], [4152,4.6891966], [1695,4.6855106], [2669,4.627697], [4280,4.5552926]]|
|37489 |[[2526,6.30834], [1109,6.0917225], [4428,6.0520463], [1879,5.9919004], [159,5.674726], [667,5.6668615], [4400,5.5806684], [3895,5.556241], [31,5.5436687], [860,5.520488]]      |
|60769 |[[3924,4.300673], [1773,4.016142], [2062,3.915859], [1915,3.8827658], [3381,3.8777452], [1559,3.8684933], [614,3.8510444], [3247,3.8390226], [2548,3.8332512], [722,3.8314707]] |
|85349 |[[2879,4.719161], [4403,4.7125244], [2611,4.71171], [498,4.554102], [3789,4.5500054], [4443,4.464675], [1376,4.4480753], [2964,4.4466925], [3209,4.411021], [3524,4.3701386]]   |
|88599 |[[3501,4.6456423], [1785,4.6396513], [3518,4.637819], [3895,4.5829515], [1915,4.5766234], [1109,4.516519], [1213,4.5118227], [476,4.5084014], [1271,4.5067496], [3259,4.503047]]|
|93319 |[[498,4.302509], [2211,4.2354617], [1109,4.222698], [4041,4.164315], [1335,4.13878], [1813,4.0863943], [933,4.0508394], [2782,4.0241103], [2861,3.9866767], [1076,3.8776183]]   |
|99239 |[[1705,5.426899], [933,4.9523816], [1977,4.8890953], [2879,4.86548], [2438,4.7922816], [3524,4.7324734], [3381,4.7261534], [4075,4.676754], [4129,4.671997], [3005,4.661511]]   |