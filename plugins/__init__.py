from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin

import operators

# Defining the plugin class
class DvdrentalPlugins(AirflowPlugin):
    name = "dvdrental_plugin"
    operators = [
        operators.RedshiftCheckTables
    ]