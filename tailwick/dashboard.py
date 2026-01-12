from flask import Blueprint,render_template
from flask_login import login_required
from flask import current_app as app


dashboard = Blueprint('dashboard',__name__,template_folder='templates',
    static_folder='static')
    

@dashboard.route('/<string:template_name>')
def dynamic_template_dashboards_view(template_name):
    return render_template(f'dashboard/{template_name}.html')

@dashboard.route('/')
def index():
    return render_template('dashboard/index.html')

