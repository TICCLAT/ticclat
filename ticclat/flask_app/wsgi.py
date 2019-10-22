from ticclat.flask_app.flask_app import create_app

flask_app = app = create_app()

if __name__ == "__main__":
    app.run(host='0.0.0.0')
