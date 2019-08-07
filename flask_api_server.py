# publishing machine learning api with python spark
from flask import Flask, jsonify, request, json
from flask_cors import CORS
import sklearn

import pickle

# initialize flask app
app = Flask("Api for model prediction")

# predicting for diabeties dataset
# https://www.kaggle.com/deepak6446/daibates-dataset-using-knn/output?scriptVersionId=18451630
model = pickle.load(open("model.sav", 'rb'))

@app.route("/predict", methods=["POST"])
def predict():
    
    data = request.json
    print("--------->", data)

    index = ['Pregnancies', 'Glucose', 'BloodPressure', 'SkinThickness', 'Insulin',
       'BMI', 'DiabetesPedigreeFunction', 'Age']
    input = [[data[index[i]] for i in range(0, len(index))]]
    print(input)

    predict = model.predict(input)

    response = app.response_class(
        response=json.dumps({"daibates": str(predict[0])}),
        status=200,
        mimetype='application/json'
    )
    return response

# Start the server, continuously listen to requests.
if __name__=="__main__":
    # For local development, set to True:
    app.run(debug=True)
    # For public web serving:
    #app.run(host='0.0.0.0')    
    app.run()

