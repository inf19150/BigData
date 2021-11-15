from airflow.plugins_manager import AirflowPlugin
from operators.http_download_operator import *

class HttpDownloadPlugin(AirflowPlugin):
    name = "http_download_operations"
    operators = [HttpDownloadOperator, OCIDDownloadOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
