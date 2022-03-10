from flask import Flask
from api.driver_server import init_driver_server
from core.driver import Driver

app = Flask(__name__)
n_mappers = 4
n_reducers = 2
i_path = '/input/dir/path'
m_path = '/middle/dir/path'
o_path = '/output/dir/path'

driver = Driver(n_mappers, n_reducers, i_path, m_path, o_path)
init_driver_server(app, driver)

if __name__ == '__main__':
    app.run()