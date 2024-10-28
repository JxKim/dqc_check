from conn_properties import DevConfig,ProdConfig,TestConfig,BaseConfig
class MyApp:
    """
    创建一个全局的app对象，用于传递上下文
    """
    def __init__(self,config_name:BaseConfig):
        """
        用于创建一个配置对象
        :param config_name:
        """
        self.config=config_name
# 在不同的环境当中，使用不同的配置项
app=MyApp(DevConfig())

