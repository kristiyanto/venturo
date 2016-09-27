from app import app
from flask import render_template
app = Flask(__name__, template_folder='templates')


@app.route('/')
@app.route('/index')
def index():
    return "Hello, World!"
@app.route('/map')
def map():
    return render_template("index.html")


if __name__ == '__main__':
    app.run(debug=True)
