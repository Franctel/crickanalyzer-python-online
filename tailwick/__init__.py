import json
import time
import sys
from pathlib import Path
from flask import Flask, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, current_user
import sqlalchemy.exc

db = SQLAlchemy()


def load_db_uri():
    """Load MySQL database URI from config.json."""
    config_path = Path(__file__).resolve().parent / "config.json"
    with open(config_path, "r") as f:
        cfg = json.load(f)

    username = cfg.get("username", "cricketanalyzer_dbLite")
    password = cfg.get("password", "dbLiteuser")
    host = cfg.get("host", "97.74.87.222")
    database = cfg.get("database", "cricketanalyzer_dbLite")

    # ✅ SQLAlchemy URI format for MySQL
    return f"mysql+mysqlconnector://{username}:{password}@{host}/{database}"


def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = b'_5#y2L"F4Q8z\n\xec]/'
    app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'

    # ✅ Load database config
    app.config['SQLALCHEMY_DATABASE_URI'] = load_db_uri()
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    db.init_app(app)

    # ---------------------------
    # Flask-Login Configuration
    # ---------------------------
    login_manager = LoginManager(app)
    login_manager.login_view = 'pages.login'   # Redirect here if not logged in
    login_manager.login_message = "Please log in to access this page."
    login_manager.login_message_category = "warning"

    # ✅ Retry DB connection (up to 60s)
    retries = 12
    while retries > 0:
        try:
            with app.app_context():
                from .models import User
                db.create_all()
            print("✅ Connected to MySQL online server.")
            break
        except sqlalchemy.exc.OperationalError:
            print(f"⚠️ MySQL not ready, retrying... ({13 - retries}/12)")
            time.sleep(5)
            retries -= 1
    else:
        print("❌ Could not connect to MySQL after 60s. Exiting.")
        sys.exit(1)

    # ---------------------------
    # Flask-Login User Loader
    # ---------------------------
    @login_manager.user_loader
    def load_user(user_id):
        from .models import User
        user = User.query.get(int(user_id))
        print(f"🔄 user_loader called with id={user_id}, found={user}")
        return user

    # ---------------------------
    # Register Blueprints
    # ---------------------------
    from .dashboard import dashboard
    from .landing import landing
    from .apps import apps
    from .pages import pages
    from .components import components

    app.register_blueprint(landing, url_prefix="/")
    app.register_blueprint(apps, url_prefix="/")
    app.register_blueprint(pages, url_prefix="/")
    app.register_blueprint(components, url_prefix="/")

    # ---------------------------
    # Root Route Handling
    # ---------------------------
    @app.route("/")
    def root_redirect():
        print("🌐 root_redirect called. Redirecting to /apps/apps-chat by default.")
        return redirect("/apps/apps-chat")

    # ---------------------------
    # Enforce Login on All /apps Routes
    # ---------------------------
    @app.before_request
    def require_login_for_protected_routes():
        from flask import request

        # Protect everything under /apps except static assets or login-related routes
        if request.path.startswith("/apps"):
            if not current_user.is_authenticated:
                print(f"🚫 Unauthorized access attempt to {request.path}")
                return redirect(url_for("pages.login"))

    return app
