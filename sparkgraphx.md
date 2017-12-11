## Written by Jose Roberto Andres

#### In Class Problems

1. Number of Distinct Names = 22547
2. Top 10 most common DescriptorNames

(Humans,117261)
(Female,42298)
(Male,39738)
(Animals,37379)
(Adult,25642)
(Middle Aged,20427)
(Aged,14201)
(Adolescent,11319)
(Child,11035)
(Rats,9767)

3. Top 10 Most common Major Descriptor Names

(Research,1649)
(Disease,1349)
(Neoplasms,1123)
(Tuberculosis,1066)
(Public Policy,816)
(Jurisprudence,796)
(Demography,763)
(Population Dynamics,753)
(Economics,690)
(Medicine,682)

4. Total number of Pair DescriptorNames = 254172331
5. Number of Pair Descriptor Names = 1813717
6. The graph of term pairs should not be directed because we want every descriptor name in a medline citation to be fully connected to one another (two way relationship). However, we do not want medline citations to be related to one another. Thus our graph should have a graph for each medline citation 

#### Out of Class - Total Data Set

1. Number of connected components = 2
2. Top words by page rank:

(54,(Humans,982.4624843076203))
(64,(Female,464.85680940879513))
(160,(Male,454.7974376929744))
(105,(Animals,432.38518343206965))
(313,(Adult,285.5078352862433))
(8181,(Middle Aged,234.2961794369222))
(371,(Aged,161.87785128679317))
(291,(Adolescent,130.8456308973281))
(784,(Rats,122.90337882388623))
(186,(Child,113.07546053912417))
(348,(Mice,97.77449975103828))
(2751,(Time Factors,78.50541305821305))
(12451,(Molecular Sequence Data,73.51875720717725))
(180,(United States,63.63789937139045))
(3841,(Child, Preschool,61.16196207419999))
(8,(Pregnancy,60.135662198485285))
(445,(Infant,54.85542031080188))
(10709,(Base Sequence,54.803110381275545))
(5262,(Amino Acid Sequence,47.35292498016865))
(796,(In Vitro Techniques,46.65680401481905))

3. Histogram of the degree distribution is file named "All Topics Degree Distribution Histogram.png" in folder called "sparkgraphxHistograms"

4. Shortest Path between terms:
    a. Pregnancy; Esophagus = 1 or (77,Map(8 -> 1)) 
    b. Femoral Artery; Electroencephalography = 2 or (172,Map(96 -> 2))
    c. Taxes; Guinea Pigs = 2 or (320,Map(185 -> 2))

#### Out of Class - Major Topic Data Set
1. Number of connected components = 878 
2. Top words by page rank:

(8,(Research,123.92049883697807))
(56,(Disease,93.7793852436392))
(28,(Neoplasms,63.26567883655364))
(265,(Tuberculosis,38.81349099214099))
(4477,(Economics,35.42027803224361))
(7375,(Public Policy,35.29638286255153))
(3644,(Population Dynamics,33.44743022305103))
(169,(Jurisprudence,32.26124076686496))
(2358,(Socioeconomic Factors,31.83659514254056))
(66,(Blood,31.031286541955946))
(3260,(Demography,29.966980925423023))
(2776,(Pharmacology,28.298937563159715))
(17,(Anesthesia,26.43689021758953))
(9234,(Social Change,24.698837716348148))
(7713,(Evaluation Studies as Topic,22.478600445035408))
(6422,(Emigration and Immigration,21.841059165697196))
(2637,(Politics,21.745228280314436))
(7702,(Family Planning Services,21.392978920581104))
(7948,(Toxicology,20.856626521688206))
(18,(Contraception,20.51285761265307))

3. Histogram of the degree distribution is file named "Major Topics Degree Distribution Histogram.png" in folder called "sparkgraphxHistograms"

4. Shortest Path between terms:
    a. Pregnancy; Esophagus = 2 or (43,Map(5450 -> 2))
    b. Femoral Artery; Electroencephalography = 2 or (1682,Map(58 -> 2))
    c. Taxes; Guinea Pigs = 3 or (8673,Map(131 -> 3))


