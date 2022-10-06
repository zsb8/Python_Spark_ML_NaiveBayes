Data is pubic.   

# Python_Spark_ML_NaiveBayes   


Use NaiveBeyes algorithm to machine learning, find the best mode to predict the validation of website.

Running environment is Spark + Hadoop + PySpark    
Used the algorithm is NavieBayes.     
Used the library is pyspark.mllib. 

# Stage1:  Read data
Placed the tsv on hadoop. Built 3 data sets: (1) Train data, (2) Validation data, (3) Sub_test data.


## Compare the parameters 
Draw the graph for the numIterations. The AUC is the highest when lambadParam is 1. But the AUCs are similar, only little difference.
~~~
    lambda_param_list = [1.0, 3.0, 5.0, 15.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 60.0]
~~~
![image](https://user-images.githubusercontent.com/75282285/194415461-55b6b713-f8d0-4270-bb27-bf934dc2c02f.png)


# Stage2: Train and evaluate   
Created the model using train data set.   
Calculated the AUC using validation data set.
Sorted the metrics.    
Found the best parameters includ the best AUC and the best model.   
![image](https://user-images.githubusercontent.com/75282285/194415598-5c0fcd28-e22e-475e-9e47-644fb7939240.png)



# Stage3: Test
Used the sub_test data set and the best model to calculate the AUC. If testing AUC is similare as the best AUC, it is OK.
As the result, the best AUC is  0.6605, use the test data set to calcuate AUC is 0.6630, the difference is 0.0025, so it has not overfitting issue. 
![image](https://user-images.githubusercontent.com/75282285/194415537-55f165d1-686f-402e-b690-a799f38347eb.png)



# Stage4: Predict
Use the test data (in Hadoop, test.tsv) and the model (calculated after Stage2) to predict.
~~~
def predict_data(best_model):
    raw_data_with_header = sc.textFile(path + "test.tsv")
    header = raw_data_with_header.first()
    raw_data = raw_data_with_header.filter(lambda x: x != header)
    r_data = raw_data.map(lambda x: x.replace("\"", ""))
    lines_test = r_data.map(lambda x: x.split('\t'))
    data_rdd = lines_test.map(lambda x: (x[0], extract_features(x, categories_map, len(x))))
    dic_desc = {
        0: 'temp web',
        1: 'evergreen web'
    }
    for data in data_rdd.take(10):
        result_predict = best_model.predict(data[1])
        print(f"web:{data[0]}, \n predict:{result_predict}, desc: {dic_desc[result_predict]}")
~~~
![image](https://user-images.githubusercontent.com/75282285/194415722-0d557064-afcc-4e95-88cf-a7e482fcc5e7.png)



# Spark monitor
http://node1:4040/jobs/
![image](https://user-images.githubusercontent.com/75282285/194416391-4b6e3eb8-c9af-471c-b544-1f48b89e2caa.png)

http://node1:8080/
![image](https://user-images.githubusercontent.com/75282285/194416117-483a554f-0f67-4205-a1dc-7f2343e6f3f4.png)
