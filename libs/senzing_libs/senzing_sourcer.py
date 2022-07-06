import json

from libs.senzing_libs.senzing_init import SenzingInit


class SenzingSourcer(SenzingInit):

    def __init__(self, logger, datasource):

        self.datasource = datasource
        super(SenzingSourcer, self).__init__(logger)

    def __get_current_def_config(self):
        config_id_bytearray = bytearray()
        self.g2_config_mgr.getDefaultConfigID(config_id_bytearray)
        self.logger.info(f"Default config ID loaded. Configuration "
                         f"ID: {config_id_bytearray.decode()}")

        response_bytearray = bytearray()
        self.g2_config_mgr.getConfig(config_id_bytearray, response_bytearray)
        self.logger.info("Default config loaded.")
        return response_bytearray

    def __add_data_source_to_config(self):
        self.logger.info(f'New data source name: {self.datasource.upper()}')

        config_bytearray = self.__get_current_def_config()
        config_handle = self.g2_config.load(config_bytearray)

        # Add Data Source
        datasource_json = f'{{"DSRC_CODE": "{self.datasource.upper()}"}}'
        response_bytearray = bytearray()
        try:
            self.g2_config.addDataSource(config_handle, datasource_json,
                                         response_bytearray)
            self.logger.info(f'Datasource {self.datasource} '
                             f'successfully added')
        except Exception as e:
            if 'already exists' in repr(e):
                self.logger.warning(repr(e))
                return

        # Save
        response_bytearray = bytearray()
        self.g2_config.listDataSources(config_handle, response_bytearray)
        total_amount = str(
            len(json.loads(response_bytearray.decode())["DATA_SOURCES"]))
        self.logger.info(f'Total amount of data sources: {total_amount}')
        response_bytearray = bytearray()
        self.g2_config.save(config_handle, response_bytearray)
        senzing_model_config_json = response_bytearray.decode()
        self.logger.info('Config saved.')

        # Cleanup
        # clear exception
        self.g2_config.clearLastException()
        # close
        self.g2_config.close(config_handle)
        # destroy
        self.g2_config.destroy()

        self.__add_new_config(senzing_model_config_json)

    def __add_new_config(self, senzing_model_config_json):

        if not senzing_model_config_json:
            self.logger.info('New config is empty')

        # add new config
        config_id_bytearray = bytearray()
        config_comment = "New configuration."
        self.g2_config_mgr.addConfig(
            senzing_model_config_json,
            config_comment,
            config_id_bytearray)
        self.logger.info(f"New config added, "
                         f"ID: {config_id_bytearray.decode()}")

        # set as default
        self.g2_config_mgr.setDefaultConfigID(config_id_bytearray)
        self.logger.info('New configuration set as default')

    def run(self):
        try:
            # get list of data source names from first line of each file
            self.logger.info('Loading data source names from filenames list')
            self.init_default_config()
            self.__add_data_source_to_config()
        except Exception as e:
            self.logger.exception(f'ERROR: {e}')
            raise e
