from flask import Blueprint,render_template
from flask_login import login_required


landing = Blueprint('landing',__name__,template_folder='templates',
    static_folder='static')
    

@landing.route('/landing/<string:template_name>')
def dynamic_template_landing_view(template_name):
    print("template_name",template_name)
    return render_template(f'landing/{template_name}.html')