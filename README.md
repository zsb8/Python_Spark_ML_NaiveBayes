Data is pubic.   

# Python_Spark_ML_NaiveBayes   


Use SVM algorithm to machine learning, find the best mode to predict the validation of website.

Running environment is Spark + Hadoop + PySpark    
Used the algorithm is NavieBayes.     
Used the library is pyspark.mllib. 

# Stage1:  Read data
Placed the tsv on hadoop. Built 3 data sets: (1) Train data, (2) Validation data, (3) Sub_test data.


## Compare the parameters 
Set the step_size=10 and reg_param=1, draw the graph for the numIterations. The AUC is the highest when num_iteration is 25. But the AUCs are similar, only little difference.
~~~
    lambda_param_list = [1.0, 3.0, 5.0, 15.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 60.0]
~~~
![image](https://user-images.githubusercontent.com/75282285/194414465-29fe7510-c5cc-4cc3-ab65-cafa6644b50f.png)


# Stage2: Train and evaluate   
Created the model using train data set.   
Calculated the AUC using validation data set.
Sorted the metrics.    
Found the best parameters includ the best AUC and the best model.   
![image](https://user-images.githubusercontent.com/75282285/194396120-13d6c770-5c42-415f-b3a6-f913cd98c406.png)



# Stage3: Test
Used the sub_test data set and the best model to calculate the AUC. If testing AUC is similare as the best AUC, it is OK.
As the result, the best AUC is  0.6605, use the test data set to calcuate AUC is 0.6630, the difference is 0.0025, so it has not overfitting issue. 
![image](https://user-images.githubusercontent.com/75282285/194395839-faba8d2d-fe4e-4f33-bc97-1afd1d4374b8.png)



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
![image](https://user-images.githubusercontent.com/75282285/194396279-6878ae32-fff0-4418-a592-f561795f1002.png)


# Spark monitor
http://node1:4040/jobs/
![image](https://user-images.githubusercontent.com/75282285/194400341-b94d79be-897e-49ab-82a7-6007a9a4c524.png)  
![image](https://user-images.githubusercontent.com/75282285/194401311-8d34a31b-6a6d-4c84-86d6-aec699421138.png)

http://node1:8080/
![image](https://user-images.githubusercontent.com/75282285/194397049-f87a9560-a343-49ea-8c22-dc557dda6de7.png)
