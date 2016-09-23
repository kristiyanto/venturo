from flask import Blueprint, render_template

main = Blueprint('main', __name__)


def index(name=None):
    return render_template('index.html', name=name)
