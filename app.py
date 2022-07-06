from flask import Flask, request
from datetime import datetime
import json
import time

from libs.utils import get_logger, get_q_manager
from libs.senzing_libs.senzing_sourcer import SenzingSourcer
from libs.senzing_libs.senzing_mapping import SenzingMapper
from libs.conf import RABBIT_Q, RABBIT_Q_ERROR

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
    else:
        try:
            start_time = datetime.now()
            q_manager = get_q_manager(RABBIT_Q, RABBIT_Q_ERROR)

            logger.info(f'Processing file: {file}')
            logger.info(f'Data Source: {datasource}')
            total = 0
            for line in file:
                line = line.strip()
                data = json.loads(line)
                i = 5
                err = None
                while i > 0:
                    try:
                        q_manager.basic_publish(
                            exchange='',
                            routing_key=RABBIT_Q,
                            body=json.dumps(data))
                        total += 1
                        break
                    except Exception as e:
                        err = e
                        time.sleep(i * 5)
                        i -= 1
                else:
                    logger.error(f'Error send message to Rabbit: {err}')
                    q_manager.basic_publish(
                        exchange='',
                        routing_key=RABBIT_Q_ERROR,
                        body={'task': json.dumps(data), 'error': err})

            logger.info(f'{total} records processed! Time spent: '
                        f'{datetime.now() - start_time}')
            return {'Status': 'OK'}
        except Exception as e:
            logger.exception(f'ERROR: {e}')
            return {'Error': repr(e)}

    return {'Status': 'OK'}


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8888, debug=True)
