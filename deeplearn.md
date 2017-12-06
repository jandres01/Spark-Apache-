## Written by Jose Roberto Andres

#### Out Of Class



1. a) AdmissionAnon2.tsv with 5 labels 

2 hidden layers, Act: Tanh,

 # of classes:    5
 Accuracy:        0.4971
 Precision:       0.4941	(2 classes excluded from average)
 Recall:          0.3166
 F1 Score:        0.5052	(2 classes excluded from average)
Precision, recall & F1: macro-averaged (equally weighted avg. of 5 classes) 

By increasing the number of inputs of the hidden layer to 5 I discovered improved accuracy rates. Lets see about adding an additional hidden layer and hopefully not overfitting

 # of classes:    5
 Accuracy:        0.5223
 Precision:       0.5234	(2 classes excluded from average)
 Recall:          0.3301
 F1 Score:        0.5331	(2 classes excluded from average)
Precision, recall & F1: macro-averaged (equally weighted avg. of 5 classes)

Adding a hidden layer has proved to decrease accuracy of the network.

 # of classes:    5
 Accuracy:        0.4535
 Precision:       0.4602	(2 classes excluded from average)
 Recall:          0.2829
 F1 Score:        0.4656	(2 classes excluded from average)
Precision, recall & F1: macro-averaged (equally weighted avg. of 5 classes)

I attempted as well to add Stochastic Gradient Descent Optimization function for the NN. But noticed poorer results.

 # of classes:    5
 Accuracy:        0.4932
 Precision:       0.4930	(2 classes excluded from average)
 Recall:          0.3107
 F1 Score:        0.5005	(2 classes excluded from average)
Precision, recall & F1: macro-averaged (equally weighted avg. of 5 classes)

I changed the learning rate as well but noticed poorer performance than 0.1

 # of classes:    5
 Accuracy:        0.4942
 Precision:       0.5002	(2 classes excluded from average)
 Recall:          0.3096
 F1 Score:        0.4979	(2 classes excluded from average)
Precision, recall & F1: macro-averaged (equally weighted avg. of 5 classes) 

From these examples I learned that for Admission2.tsv it is best to increase the input value for the hidden layer, learning rate of 0.1 and backprop does provide best accuracy for this test data.

b. AdmissionAnon3.tsv with 2 labels



 # of classes:    2
 Accuracy:        0.8343
 Precision:       0.7670
 Recall:          0.7077
 F1 Score:        0.8979

For Admission3 I decided to see how the NN would perform with stochastic gradient descent. Saw it performed much more poorly

 # of classes:    2
 Accuracy:        0.8023
 Precision:       0.7142
 Recall:          0.7116
 F1 Score:        0.8731

I attempted to change the activation from TANH to RELU but noticed poorer performances

# of classes:    2
 Accuracy:        0.7936
 Precision:       0.7007
 Recall:          0.6899
 F1 Score:        0.8684

I wanted to see once again how the network performed with an additional hidden layer but noticed poorer performance.

 # of classes:    2
 Accuracy:        0.7665
 Precision:       0.6722
 Recall:          0.6707
 F1 Score:        0.8481

Funny part is if I remove the hidden layers the network performs relatively descent compared to base result

 # of classes:    2
 Accuracy:        0.8110
 Precision:       0.7297
 Recall:          0.6925
 F1 Score:        0.8816

The best classification I got had an accuracy of 83.43%
