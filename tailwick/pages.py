from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from flask_login import login_user, logout_user, login_required, current_user
from .models import User  # SQLAlchemy User model
import bcrypt

pages = Blueprint('pages', __name__, template_folder='templates', static_folder='static')

# -------------------------
# Dynamic template routes
# -------------------------
@pages.route('/pages/authentication/<string:template_name>')
def dynamic_template_authentication_view(template_name):
    return render_template(f'pages/authentication/{template_name}.html')


@pages.route('/pages/pages/<string:template_name>')
def dynamic_template_pages_view(template_name):
    return render_template(f'pages/pages/{template_name}.html')


# -------------------------
# Authentication
# -------------------------
@pages.route('/account/login', methods=['GET', 'POST'])
def login():
    from flask import session, get_flashed_messages

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        print(f"🔍 Login attempt: username={username}")

        user = User.query.filter_by(user_name=username).first()

        if not user:
            session.pop('_flashes', None)  # clear any old flash messages
            flash("Invalid username", "danger")
            return redirect(url_for('pages.login'))

        try:
            stored_hash = user.login_password.encode("utf-8")
            entered_password = password.encode("utf-8")

            if not bcrypt.checkpw(entered_password, stored_hash):
                session.pop('_flashes', None)
                flash("Invalid password", "danger")
                return redirect(url_for('pages.login'))

        except Exception as e:
            session.pop('_flashes', None)
            flash("Error verifying password", "danger")
            print(f"⚠️ Password check error: {e}")
            return redirect(url_for('pages.login'))

        # ✅ Login success
        login_user(user, remember=True)
        session.pop('_flashes', None)
        flash("Login successful!", "success")

        if getattr(user, "association_id", None):
            session['association_id'] = user.association_id

        return redirect("/apps/apps-chat")

    if current_user.is_authenticated:
        return redirect("/apps/apps-chat")

    return render_template('pages/account/login.html')


@pages.route('/account/logout')
@login_required
def logout():
    from flask import session
    print(f"👋 Logout: user_id={current_user.id if current_user else None}")

    logout_user()
    session.pop("association_id", None)
    session.pop('_flashes', None)  # ✅ clear leftover login messages
    flash("You have been logged out.", "info")

    return redirect(url_for("pages.login"))

