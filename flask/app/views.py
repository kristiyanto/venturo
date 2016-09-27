from app import app
from flask import render_template, Flask

@app.route('/')
@app.route('/index')
def index():
    return "Hello, World!"
    # return render_template("index.html")
@app.route('/map')
def map():
    return render_template("index.html")

