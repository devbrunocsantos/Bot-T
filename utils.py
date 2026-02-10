import os

def configurar_ambiente_proxy(proxy_host=None, proxy_porta=None, proxy_user=None, proxy_pass=None):
    """
    Configura as variáveis de ambiente para o proxy.
    """
    print("Configurando variáveis de ambiente para o proxy...")
    os.environ['WDM_SSL_VERIFY'] = '0'
    os.environ['NO_PROXY'] = 'localhost,127.0.0.1'
    
    if proxy_host and proxy_porta:
        if proxy_user and proxy_pass:
            proxy_url = f"http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_porta}"
        else:
            proxy_url = f"http://{proxy_host}:{proxy_porta}"

        os.environ['HTTP_PROXY'] = proxy_url
        os.environ['HTTPS_PROXY'] = proxy_url
        print(f"Proxy configurado.")