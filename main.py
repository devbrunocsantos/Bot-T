import os
import time
from datetime import datetime
from collections import Counter
import requests
from configs.config import LOGGER, BRL_USD_RATE
from tools.database import DataManager
from tools.strategy import CashAndCarryBot

def get_live_usd_brl():
    """
    Busca a cotação atual do Dólar Comercial (USD-BRL) via API pública.
    Retorna o valor 'bid' (compra). Em caso de erro, retorna o fixo do config.
    """
    try:
        # Endpoint da AwesomeAPI (Atualiza a cada 30s)
        response = requests.get("https://economia.awesomeapi.com.br/last/USD-BRL", timeout=5)
        data = response.json()
        rate = float(data['USDBRL']['bid'])
        LOGGER.info(f"Cotação USD/BRL obtida: R$ {rate:.4f}")
        return rate
    except Exception as e:
        LOGGER.error(f"Falha ao obter cotação real: {e}. Usando fallback: {BRL_USD_RATE}")
        return BRL_USD_RATE

def main():
    LOGGER.info("Iniciando Bot Cash & Carry (Modo Simulado)...")
    
    # Inicialização do Bot
    bot = CashAndCarryBot(initial_capital_usd=1000.0) 
    
    # Variáveis de controle de tempo
    last_scan_time = 0
    scan_interval = 3600 # 1 hora
    last_deposit_check = time.time()
    deposit_interval = 2592000 # 30 dias (Simulação simplificada)

    # --- [ALTERADO] Configuração do Banco de Dados (INÍCIO) ---
    # Instancia a conexão fora do loop para manter persistência e eficiência
    db_dir = "databases"
    os.makedirs(db_dir, exist_ok=True)
    
    # Define o mês atual para controle
    current_month = datetime.now().strftime('%m-%Y') 
    db_name = f"database_{current_month}.db"
    db_path = os.path.join(db_dir, db_name)

    db_manager = DataManager(db_name=db_path)
    LOGGER.info(f"Conectado ao banco de dados: {db_name}")
    # --- [ALTERADO] Configuração do Banco de Dados (FIM) ---

    try:
        while True:
            current_time = time.time()

            # --- [NOVO] Verificação de Rotação de Mês ---
            # Se o mês mudou, fecha o DB atual e abre um novo
            new_month = datetime.now().strftime('%m-%Y')
            if new_month != current_month:
                LOGGER.info(f"Virada de mês detectada ({current_month} -> {new_month}). Rotacionando DB...")
                db_manager.close()
                
                current_month = new_month
                db_name = f"database_{current_month}.db"
                db_path = os.path.join(db_dir, db_name)
                
                db_manager = DataManager(db_name=db_path)
            # ---------------------------------------------

            # 1. Simulação de Aporte Mensal
            if current_time - last_deposit_check > deposit_interval:
                current_rate = get_live_usd_brl()
                bot.deposit_monthly_contribution(exchange_rate=current_rate)
                last_deposit_check = current_time

            # 2. Lógica de Mercado
            if bot.position is None:
                # Se não tem posição, escaneia
                if current_time - last_scan_time > scan_interval:
                    top_pairs = bot.get_top_volume_pairs()

                    # Variáveis para estatísticas do log de scanner
                    best_fr = -100.0
                    best_pair = None
                    reasons = []
                    final_reason = "ENTRY_EXECUTED"

                    # Se a lista estiver vazia, evita erro no loop
                    if not top_pairs:
                        LOGGER.warning("Nenhum par encontrado no filtro de volume.")
                        final_reason = "NO_VOLUME"
                    
                    for pair in top_pairs:
                        is_viable, fr, reason = bot.check_entry_opportunity(pair)

                        if fr > best_fr:
                            best_fr = fr
                            best_pair = pair

                        reasons.append(reason)

                        if is_viable:
                            success = bot.simulate_entry(pair, fr)
                            if success:
                                break # Entra em apenas uma posição por vez
                        else:
                            # Se não entrou, define o motivo mais comum para log
                            if reasons:
                                final_reason = Counter(reasons).most_common(1)[0][0]

                    # Garante que best_pair tenha valor para o log mesmo se não houve entrada
                    if best_pair is None and top_pairs:
                         best_pair = top_pairs[0]

                    db_manager.log_scan_attempt({
                        'total_analyzed': len(top_pairs),
                        'passed_volume': len(top_pairs),
                        'best_funding': best_fr,
                        'best_pair': best_pair,
                        'reason': final_reason
                    })

                    last_scan_time = current_time
            else:
                # Se tem posição, monitora (agora com persistência e cash flow real)
                bot.monitor_and_manage(db_manager)

            # Aguarda próximo ciclo (evita flood de CPU/API)
            time.sleep(60) 

    except KeyboardInterrupt:
        LOGGER.info("Parando bot manualmente...")
    except Exception as e:
        LOGGER.critical(f"Erro fatal no loop principal: {e}")
    finally:
        # [NOVO] Garante o fechamento limpo da conexão
        try:
            db_manager.close()
            LOGGER.info("Conexão com banco de dados encerrada.")
        except:
            pass

if __name__ == "__main__":
    main()