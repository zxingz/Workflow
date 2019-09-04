# This is the class you derive to create a plugin
from airflow.plugins_manager import AirflowPlugin
from flask_admin.base import MenuLink

Dashboard = MenuLink(
    category='Flower',
    name='Dashboard',
    url='/dashboard')

Tasks = MenuLink(
    category='Flower',
    name='Tasks',
    url='/tasks')

Broker = MenuLink(
    category='Flower',
    name='Broker',
    url='/broker')

Monitor = MenuLink(
    category='Flower',
    name='Monitor',
    url='/monitor')

# Defining the plugin class
class FlowerPlugin(AirflowPlugin):
    name='flower_plugin'
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = [Dashboard, Tasks, Broker, Monitor]
    operators = []
    # A list of class(es) derived from BaseHook
    hooks = []
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []