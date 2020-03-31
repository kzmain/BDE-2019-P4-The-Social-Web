from flask import Flask, render_template, request
from DataProcessor.DataProcessor import DataProcessor as Dp
import os

java8_location = '/Library/Java/JavaVirtualMachines/liberica-jdk-1.8.0_202/Contents/Home'  # Set your own
os.environ['JAVA_HOME'] = java8_location

app = Flask(__name__)
data = Dp()


true_x_axis_data=data.get_true_x_axis_data()
true_data_estimation_hillary=data.get_true_data_estimation_hillary()
true_data_estimation_trump=data.get_true_data_estimation_trump()
false_data_estimation_hillary=data.get_false_data_estimation_hillary()
false_data_estimation_trump=data.get_false_data_estimation_trump()

true_data_positive_trump=data.get_true_data_positive_trump()
true_data_neutral_trump=data.get_true_data_neutral_trump()
true_data_negative_trump=data.get_true_data_negative_trump()
true_data_positive_hillary=data.get_true_data_positive_hillary()
true_data_neutral_hillary=data.get_true_data_neutral_hillary()
true_data_negative_hillary=data.get_true_data_negative_hillary()
true_data_heat_trump=data.get_true_data_heat_trump()
true_data_heat_hillary=data.get_true_data_heat_hillary()
true_data_word_cloud=data.get_true_data_word_cloud()


@app.route('/')
def index():
    return render_template(
        'index.html',
        true_x_axis_data=true_x_axis_data,
        true_data_estimation_hillary=true_data_estimation_hillary,
        true_data_estimation_trump=true_data_estimation_trump,

        false_data_estimation_hillary=false_data_estimation_hillary,
        false_data_estimation_trump=false_data_estimation_trump,

        true_data_positive_trump=true_data_positive_trump,
        true_data_neutral_trump=true_data_neutral_trump,
        true_data_negative_trump=true_data_negative_trump,
        true_data_positive_hillary=true_data_positive_hillary,
        true_data_neutral_hillary=true_data_neutral_hillary,
        true_data_negative_hillary=true_data_negative_hillary,
        true_data_heat_trump=true_data_heat_trump,
        true_data_heat_hillary=true_data_heat_hillary,
        true_data_word_cloud=true_data_word_cloud
    )


@app.route('/api/true', methods=['POST'], strict_slashes=False)
def api_true():
    if request.method == "POST":
        rd = request.get_json()
        print(rd)
        from datetime import datetime
        sd = rd["start_date"] / 1000
        ed = rd["end_date"] / 1000
        sd = datetime.fromtimestamp(sd)
        ed = datetime.fromtimestamp(ed)
        return data.get_new_true_data_word_cloud(sd, ed)


@app.route('/demo')
def demo():
    return render_template("demo.html")


if __name__ == '__main__':
    app.run()
