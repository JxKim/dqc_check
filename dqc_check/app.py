from conn_properties import DevConfig,ProdConfig,TestConfig,BaseConfig
import os
class MyApp:
    """
    创建一个全局的app对象，用于传递上下文
    """
    def __init__(self,config_name:str):
        """
        用于创建一个配置对象
        :param config_name:
        """
        config_cls_map={
            'dev':DevConfig,
            'prod':ProdConfig,
            'test':TestConfig
        }
        self.config=config_cls_map.get(config_name.lower(),BaseConfig)()
# 在不同的环境当中，使用不同的配置项
app=MyApp(os.getenv('DQC_CHECK_ENV'))

