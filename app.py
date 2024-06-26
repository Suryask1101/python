from flask import Flask, request, render_template

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    result = None
    if request.method == 'POST':
        num = int(request.form.get('num'))
        if num >= 15:
            result = "yes"
        else:
            result = "no"
    return render_template('index.html', result=result)

if __name__ == "__main__":
    app.run()
