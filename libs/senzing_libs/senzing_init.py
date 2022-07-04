import json
import os
import time
from senzing import G2ConfigMgr, G2Config, G2Engine
from contextlib import suppress

from libs.conf import VERBOSE_LOG, INIT_JSON, CONFIG_JSON, FORCE_LOAD_CONFIG


class SenzingInit:

    def __init__(self, logger):

        self.logger = logger
        self.settings = None
        self.config = None
        self.g2_config_mgr = None
        self.g2_config = None
        self.g2_engine = None
        self.__init_json_settings()
        self.__init_json_config()
        self._init_senzing()

    def __init_json_config(self):
        with suppress(Exception):
            with open(CONFIG_JSON) as json_file:
                self.config = json.dumps(json.load(json_file))

    def __init_json_settings(self):
        with open(INIT_JSON) as json_file:
            self.settings = json.load(json_file)

    def _init_g2_engine(self, config_json):
        if self.g2_engine:
            return self.g2_engine

        self.g2_engine = G2Engine()
        try:
            self.g2_engine.init(
                self.settings['module_name'],
                config_json,
                VERBOSE_LOG)
            return self.g2_engine
        except Exception as e:
            self.logger.error(f"__ini_g2_engine error {e}")
            raise RuntimeError(e)

    def _init_g2_config(self, config_json):
        if self.g2_config:
            return self.g2_config

        self.g2_config = G2Config()
        try:
            self.g2_config.init(
                self.settings['module_name'],
                config_json,
                VERBOSE_LOG)
            return self.g2_config
        except Exception as e:
            self.logger.error(f"__init_g2_config error {e}")
            raise RuntimeError(e)

    def _init_g2_config_mgr(self, config_json):
        if self.g2_config_mgr:
            return self.g2_config_mgr

        self.g2_config_mgr = G2ConfigMgr()
        try:
            self.g2_config_mgr.init(
                self.settings['module_name'],
                config_json,
                VERBOSE_LOG)
            return self.g2_config_mgr
        except Exception as e:
            self.logger.error(f"__init_g2_config_mgr error {e}")
            raise RuntimeError(e)

    def _init_senzing(self):
        try:
            # paths
            data_dir = self.settings['SENZING_DATA_DIR']
            config_path = self.settings['SENZING_ETC_DIR']
            g2_dir = self.settings['SENZING_G2_DIR']
            lic_path = self.settings['LICENSEFILE']
            # create senzing config
            support_path = os.environ.get("SENZING_DATA_VERSION_DIR", data_dir)
            resource_path = os.environ.get(
                'RESOURCEPATH', "{0}/resources".format(g2_dir))
            sql_connection = self.settings['SENZING_SQL_CONNECTION']
            senzing_config_dictionary = {
                "PIPELINE": {
                    "CONFIGPATH": config_path,
                    "SUPPORTPATH": support_path,
                    "RESOURCEPATH": resource_path,
                    "LICENSEFILE": lic_path
                },
                "SQL": {
                    "CONNECTION": sql_connection,
                }
            }
            config_json = json.dumps(senzing_config_dictionary)

            self._init_g2_config_mgr(config_json)
            self._init_g2_config(config_json)
            self.init_default_config()
            self._init_g2_engine(config_json)

        except Exception as e:
            self.logger.error(f"__init_g2_config error:{e}")
            raise RuntimeError(e)

    def init_default_config(self):

        config_id_bytearray = bytearray()
        self.g2_config_mgr.getDefaultConfigID(config_id_bytearray)
        if config_id_bytearray and not FORCE_LOAD_CONFIG:
            self.logger.warning("Default config already set")
            return
        self.logger.info(
            "No default configuration set, creating "
            "one in the Senzing repository")
        if not self.config:
            # Create configuration from template file.
            config_handle = self.g2_config.create()
            # Save Senzing configuration to string.
            response_bytearray = bytearray()
            self.g2_config.save(config_handle, response_bytearray)
            self.config = response_bytearray.decode()
        # Externalize Senzing configuration to the database.
        config_comment = "senzing-init added at {0}".format(time.time())
        config_id_bytearray = bytearray()
        self.g2_config_mgr.addConfig(
            self.config,
            config_comment,
            config_id_bytearray)
        # Set new configuration as the default.
        self.g2_config_mgr.setDefaultConfigID(config_id_bytearray)

    def save_config(self):
        # Get the current configuration from the Senzing database
        default_config_id = bytearray()
        self.g2_config_mgr.getDefaultConfigID(default_config_id)

        config_current = bytearray()
        self.g2_config_mgr.getConfig(default_config_id, config_current)
        config_string = config_current.decode()

        conf = json.loads(config_string)
        with open(CONFIG_JSON, 'w') as out:
            out.write(json.dumps(conf))
        self.logger.info('Config file successfully saved')
