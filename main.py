import os
import re
import time
from datetime import datetime
from collections import Counter
import requests
from configs.config import LOGGER, BRL_USD_RATE, MIN_ORDER_VALUE_USD
from tools.database import DataManager
from tools.strategy import CashAndCarryBot

def get_live_usd_brl(bot_instance):
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

        bot_instance.update_brl_rate(rate)

        return rate
    
    except Exception as e:
        LOGGER.error(f"Falha ao obter cotação real: {e}. Usando fallback: {BRL_USD_RATE}")
        return BRL_USD_RATE

def main():
    LOGGER.info("Iniciando Cash & Carry Bot...")
    
    # Inicialização do Bot
    bot = CashAndCarryBot() 
    
    # Variáveis de controle de tempo
    last_scan_time = 0
    scan_interval = 3600 # 1 hora

    # Configuração do Banco de Dados
    db_dir = "databases"
    os.makedirs(db_dir, exist_ok=True)
    
    # Define o mês atual para controle
    current_month = datetime.now().strftime('%m-%Y') 
    db_name = f"database_{current_month}.db"
    db_path = os.path.join(db_dir, db_name)

    db_manager = DataManager(db_name=db_path)
    LOGGER.info(f"Conectado ao banco de dados: {db_name}")

    try:
        while True:
            current_time = time.time()

            # Verificação de Rotação de Mês
            new_month = datetime.now().strftime('%m-%Y')
            if new_month != current_month:
                LOGGER.info(f"Virada de mês detectada ({current_month} -> {new_month}). Rotacionando DB...")
                db_manager.close()
                
                current_month = new_month
                db_name = f"database_{current_month}.db"
                db_path = os.path.join(db_dir, db_name)
                
                db_manager = DataManager(db_name=db_path)

            # Lógica de Mercado
            if bot.position is None:
                # Se não tem posição, escaneia
                if current_time - last_scan_time > scan_interval:
                    try:
                        bot.auto_balance_wallets()
                    except Exception as e:
                        LOGGER.error(f"Falha no auto-balanceamento: {e}")

                    if (bot.capital / 2) < MIN_ORDER_VALUE_USD:
                        LOGGER.info("CAPITAL INSUFICIENTE! (< $22) Aguradando uma hora para nova tentativa...")
                        last_scan_time = current_time
                        continue

                    top_pairs = bot.get_top_volume_pairs()

                    # --- Batch Fetching Híbrido (Spot + Swaps) ---
                    all_tickers = {}
                    all_funding = {}
                    
                    if top_pairs:
                        try:
                            LOGGER.info("Baixando dados de mercado (Spot + Swaps) e Funding...")
                            
                            # 1. Busca Tickers de Futuros (onde operamos)
                            # Retorna chaves como 'BTC/USDT:USDT'
                            tickers_swap = bot.exchange_swap.fetch_tickers()
                            
                            # 2. Busca Tickers de Spot (para calcular o preço base)
                            tickers_spot = bot.exchange_spot.fetch_tickers()
                            
                            # 3. Funde os dicionários
                            # Isso garante que teremos tanto a chave 'BTC/USDT' quanto 'BTC/USDT:USDT'
                            all_tickers = {**tickers_swap, **tickers_spot}
                            
                            # 4. Busca Funding Rates
                            all_funding = bot.exchange_swap.fetch_funding_rates()
                                
                        except Exception as e:
                            LOGGER.error(f"Erro crítico ao baixar dados em lote: {e}")
                            time.sleep(10)
                            continue
                    # ------------------------------------------------------------------

                    # Variáveis para estatísticas do log de scanner
                    best_fr = -100.0
                    best_pair = None
                    reasons = []
                    final_reason = "ENTRY_EXECUTED"

                    if not top_pairs:
                        LOGGER.warning("Nenhum par encontrado no filtro de volume.")
                        final_reason = "NO_VOLUME"
                    
                    for pair in top_pairs:
                        try:
                            # Se não temos dados de funding para este par, ignoramos
                            if pair not in all_funding:
                                continue

                            # Definições Iniciais
                            # pair futura ex: 'POWER/USDT:USDT'
                            symbol_spot_candidate = pair.split(':')[0] 
                            base_swap_raw = pair.split('/')[0] # 'POWER'
                            
                            # Limpeza inteligente de prefixos numéricos (1000PEPE -> PEPE)
                            base_swap_clean = re.sub(r"^\d+", "", base_swap_raw)

                            found_spot = None
                            
                            # Busca Direta
                            if symbol_spot_candidate in tickers_spot:
                                found_spot = symbol_spot_candidate
                            
                            # --- Verificações Finais ---
                            if not found_spot:
                                reasons.append(f"MISSING_SPOT_DATA ({base_swap_clean})")
                                continue

                            price_swap = all_tickers[pair]['last']
                            price_spot = all_tickers[found_spot]['last']

                            # Obtém Funding Rate
                            fr_rate = all_funding[pair]['fundingRate']

                            # Passa os dados já processados
                            is_viable, fr, reason = bot.check_entry_opportunity(
                                pair, found_spot,
                                price_spot=price_spot, 
                                price_swap=price_swap, 
                                funding_rate=fr_rate
                            )
                        except Exception as e:
                            LOGGER.error(f"Erro ao processar par {pair}: {e}")
                            reasons.append("PROCESSING_ERROR")
                            continue

                        if fr > best_fr:
                            best_fr = fr
                            best_pair = pair

                        reasons.append(reason)

                        if is_viable:
                            # Executa entrada
                            success = bot.execute_real_entry(pair, found_spot, bot.capital)
                            if success:
                                break
                        else:
                            if reasons:
                                final_reason = Counter(reasons).most_common(1)[0][0]

                    if best_pair is None and top_pairs:
                         best_pair = top_pairs[0]

                    db_manager.log_scan_attempt({
                        'total_analyzed': len(top_pairs),
                        'passed_volume': len(top_pairs),
                        'best_funding': best_fr,
                        'best_pair': best_pair,
                        'reason': final_reason
                    })

                    LOGGER.info("Fim da varredura dinâmica de mercado.")
                    last_scan_time = current_time
            else:
                # Se tem posição, monitora
                bot.monitor_and_manage(db_manager)

            # Aguarda próximo ciclo
            time.sleep(300) 

    except KeyboardInterrupt:
        LOGGER.info("Parando bot manualmente...")
    except Exception as e:
        LOGGER.critical(f"Erro fatal no loop principal: {e}")
    finally:
        try:
            db_manager.close()
            LOGGER.info("Conexão com banco de dados encerrada.")
        except:
            pass

if __name__ == "__main__":
    main()