from flask import Blueprint,render_template
from flask_login import login_required


components = Blueprint('components',__name__,template_folder='templates',
    static_folder='static')
    

@components.route('/components/ui/<string:template_name>')
def dynamic_template_components_ui_view(template_name):
    return render_template(f'components/ui/{template_name}.html')

@components.route('/components/plugins/<string:template_name>')
def dynamic_template_components_plugins_view(template_name):
    return render_template(f'components/plugins/{template_name}.html')

@components.route('/components/navigation/<string:template_name>')
def dynamic_template_components_navigation_view(template_name):
    return render_template(f'components/navigation/{template_name}.html')

@components.route('/components/forms/<string:template_name>')
def dynamic_template_components_forms_view(template_name):
    return render_template(f'components/forms/{template_name}.html')

@components.route('/components/tables/<string:template_name>')
def dynamic_template_components_tables_view(template_name):
    return render_template(f'components/tables/{template_name}.html')

@components.route('/components/charts/<string:template_name>')
def dynamic_template_components_charts_view(template_name):
    return render_template(f'components/charts/{template_name}.html')

@components.route('/components/icons/<string:template_name>')
def dynamic_template_components_icons_view(template_name):
    return render_template(f'components/icons/{template_name}.html')

@components.route('/components/maps/<string:template_name>')
def dynamic_template_components_maps_view(template_name):
    return render_template(f'components/maps/{template_name}.html')