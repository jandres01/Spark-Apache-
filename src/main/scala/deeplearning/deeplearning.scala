package deeplearning

import org.datavec.api.records.reader.RecordReader
import org.datavec.api.records.reader.impl.csv.CSVRecordReader
import org.datavec.api.split.FileSplit
import org.datavec.api.util.ClassPathResource
import org.deeplearning4j.datasets.datavec.RecordReaderDataSetIterator
import org.deeplearning4j.eval.Evaluation
import org.deeplearning4j.nn.conf.MultiLayerConfiguration
import org.deeplearning4j.nn.conf.NeuralNetConfiguration
import org.deeplearning4j.nn.conf.layers.DenseLayer
import org.deeplearning4j.nn.conf.layers.OutputLayer
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.dataset.SplitTestAndTrain
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator
import org.nd4j.linalg.dataset.api.preprocessor.DataNormalization
import org.nd4j.linalg.dataset.api.preprocessor.NormalizerStandardize
import org.nd4j.linalg.lossfunctions.LossFunctions
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.io._
import java.io._
import org.deeplearning4j.nn.api.OptimizationAlgorithm


object DeepLearning extends App {

    var log = LoggerFactory.getLogger("DeepLearn");

    //First: get the dataset using the record reader. CSVRecordReader handles loading/parsing
    var numLinesToSkip = 0
    var delimiter = '\t'
    var recordReader = new CSVRecordReader(numLinesToSkip,delimiter)
    recordReader.initialize(new FileSplit(new File("/data/BigData/admissions/AdmissionAnon3.tsv")))

    //Second: the RecordReaderDataSetIterator handles conversion to DataSet objects, ready for use in neural network
    var labelIndex = 40;     //5 values in each row of the iris.txt CSV: 4 input features followed by an integer label (class) index. Labels are the 5th value (index 4) in each row
    var numClasses = 5;     //3 classes (types of iris flowers) in the iris data set. Classes have integer values 0, 1 or 2
    var batchSize = 2946;    //Iris data set: 150 examples total. We are loading all of them into one DataSet (not recommended for large data sets)

    var iterator = new RecordReaderDataSetIterator(recordReader,batchSize,labelIndex,numClasses);
    var allData = iterator.next(); 
    allData.shuffle();
    var testAndTrain = allData.splitTestAndTrain(0.65);  //Use 65% of data for training

    var trainingData = testAndTrain.getTrain();
    var testData = testAndTrain.getTest();

    //We need to normalize our data. We'll use NormalizeStandardize (which gives us mean 0, unit variance):
    var normalizer = new NormalizerStandardize();
    normalizer.fit(trainingData);           //Collect the statistics (mean/stdev) from the training data. This does not modify the input data
    normalizer.transform(trainingData);     //Apply normalization to the training data
    normalizer.transform(testData);         //Apply normalization to the test data. This is using statistics calculated from the *training* set

    var numInputs = 40;
    var outputNum = 5;
    var iterations = 1000;
    var seed = 6;

    println("Build model....");
    var conf = new NeuralNetConfiguration.Builder()
        .seed(seed)
        .iterations(iterations)
        .activation(Activation.TANH)
        .weightInit(WeightInit.XAVIER)
        .learningRate(0.01)
        .regularization(true).l2(1e-4)
        .list()
        .layer(0, new DenseLayer.Builder().nIn(numInputs).nOut(5)
            .build())
        .layer(1, new DenseLayer.Builder().nIn(5).nOut(3)
            .build())
        .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
            .activation(Activation.SOFTMAX)
            .nIn(3).nOut(outputNum).build())
        .backprop(true).pretrain(false)
        .build();

    //run the model
    var model = new MultiLayerNetwork(conf);
    model.init();
    model.setListeners(new ScoreIterationListener(100));

    model.fit(trainingData);

    //evaluate the model on the test set
    var eval = new Evaluation(2);
    var output = model.output(testData.getFeatureMatrix());
    eval.eval(testData.getLabels(), output);
    println(eval.stats())

}