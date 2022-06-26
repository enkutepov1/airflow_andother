# <YOUR_IMPORTS>
import dill
import pandas as pd
import json
import os
def predict():
    # <YOUR_CODE>

    with open('C:/Users/enkut/airflow-docker/dags/data/models/cars_pipe_202206211037.pkl', 'rb') as f:
        model = dill.load(f)
    g = [os.path.join(z, i) for z, x, c in os.walk('C:/Users/enkut/airflow-docker/dags/data/test/') for i in c]
    j = []
    for i in g:
        with open(i, "r") as read_file:
            data = json.load(read_file)

            df = pd.DataFrame.from_dict(data, orient='index').T
            prediction = model.predict(df)
            j.append([i[-15:-5], prediction[0]])
        #print(j)
        #return j

    df1 = pd.DataFrame(j,columns=['car_id','pred'])
    df1.to_csv(r"C:/Users/enkut/airflow-docker/dags/data/predictions/file.csv",header=True,index=False)

    # with open('C:/Users/enkut/airflow-docker/dags/data/predictions/file.csv','w', newline='') as f:
    #     writer = csv.writer(f)
    #     writer.writerow(j)



if __name__ == '__main__':
    predict()