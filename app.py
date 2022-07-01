from flask import Flask, request

from libs.utils import get_logger
from libs.senzing_libs.senzing_sourcer import SenzingSourcer
from libs.senzing_libs.senzing_mapping import SenzingMapper

logger = get_logger()
app = Flask(__name__)


@app.route('/ping', methods=['GET'])
def ping():
    return 'pong'


@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return {'Error': "No file part in request"}

    logger.info(f'Upload...')
    params = request.form
    logger.info(f'Input params: {params}')
    required = ["datasource", "is_need_mapping"]
    if not all(params.get(x) for x in required):
        return {'Error': 'Missing required parameter(s)'}

    file = request.files['file']
    datasource = params['datasource'].upper()
    if file.filename == '':
        return {'Error': 'No selected file'}

    try:
        sourser = SenzingSourcer(logger, datasource)
        sourser.run()
    except Exception as e:
        return {'Error': repr(e)}

    if params['is_need_mapping'] == 'true':
        try:
            mapper = SenzingMapper(file, logger, datasource)
            mapper.run()
        except Exception as e:
            return {'Error': repr(e)}

    return 'OK'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8888, debug=True)
