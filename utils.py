import os
from configs.config import LOGGER

def configurar_ambiente_proxy(proxy_host, proxy_porta, proxy_user, proxy_pass):
    """
    Configura variáveis de ambiente para Proxy HTTP/HTTPS.
    Crucial para redes corporativas.
    """
    if not proxy_host:
        return

    # Monta a string de conexão autenticada
    # Formato: http://user:pass@host:port
    proxy_url = f"http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_porta}"

    os.environ['HTTP_PROXY'] = proxy_url
    os.environ['HTTPS_PROXY'] = proxy_url
    
    LOGGER.info(f"Ambiente de Proxy configurado para: {proxy_host}:{proxy_porta}")