import os

from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello():
    name = os.environ.get('NAME', 'World')
    return 'Hello {}!\n'.format(name)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=int(os.environ.get('PORT')))