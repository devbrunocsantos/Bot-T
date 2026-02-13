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
    
    time.sleep(1)

    bot.start_guardian()
    
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

                    top_pairs, tickers_swap, tickers_spot = bot.get_top_volume_pairs()
                    
                    if top_pairs:
                        try:
                            LOGGER.info("Baixando dados de mercado (Spot + Swaps) e Funding...")
                                
                        except Exception as e:
                            LOGGER.error(f"Erro crítico ao baixar dados em lote: {e}")
                            time.sleep(10)
                            continue
                    # ------------------------------------------------------------------

                    # Variáveis para estatísticas do log de scanner
                    reasons = []
                    final_reason = "ENTRY_EXECUTED"

                    if not top_pairs:
                        LOGGER.warning("Nenhum par aprovado.")
                        last_scan_time = current_time
                        continue

                    viable_opportunities = []
                    unviable_opportunities = []
                    
                    for pair, fr_rate in top_pairs.items():
                        try:
                            # Definições Iniciais
                            symbol_spot_candidate = pair.split(':')[0] 
                            base_swap_raw = pair.split('/')[0]
                            
                            # Limpeza inteligente de prefixos numéricos
                            base_swap_clean = re.sub(r"^\d+", "", base_swap_raw)

                            found_spot = None
                            
                            # Busca Direta
                            if symbol_spot_candidate in tickers_spot:
                                found_spot = symbol_spot_candidate
                                price_spot = tickers_spot[found_spot]['last']
                                price_swap = tickers_swap[pair]['last']
                            else:
                                reasons.append(f"MISSING_SPOT_DATA ({base_swap_clean})")
                                continue

                            # Passa os dados já processados
                            is_viable, fr, reason = bot.check_entry_opportunity(
                                pair, found_spot,
                                price_spot=price_spot, 
                                price_swap=price_swap, 
                                funding_rate=fr_rate
                            )

                            if is_viable:
                                LOGGER.info(f"Candidato Classificado: {pair} | Funding: {fr:.4%}")
                                viable_opportunities.append({
                                    'pair': pair,
                                    'spot_symbol': found_spot,
                                    'funding_rate': fr,
                                    'price_spot': price_spot,
                                    'price_swap': price_swap
                                })
                            else:
                                unviable_opportunities.append({
                                    'pair': pair,
                                    'spot_symbol': found_spot,
                                    'funding_rate': fr,
                                    'price_spot': price_spot,
                                    'price_swap': price_swap
                                })

                            reasons.append(reason)

                        except Exception as e:
                            LOGGER.error(f"Erro ao processar par {pair}: {e}")
                            reasons.append("PROCESSING_ERROR")
                            continue

                        time.sleep(0.5)

                    if viable_opportunities:
                        best_opportunity = sorted(
                            viable_opportunities, 
                            key=lambda x: x['funding_rate'], 
                            reverse=True
                        )[0]

                        LOGGER.info("MELHOR OPORTUNIDADE:")
                        LOGGER.info(f"Par: {best_opportunity['pair']}")
                        LOGGER.info(f"Funding: {best_opportunity['funding_rate']:.4%}")

                        # Executa entrada
                        success = bot.execute_real_entry(
                            best_opportunity['pair'], 
                            best_opportunity['spot_symbol'], 
                            bot.capital
                        )

                        db_manager.log_scan_attempt({
                            'total_analyzed': len(top_pairs),
                            'passed_volume': len(top_pairs),
                            'best_funding': best_opportunity['funding_rate'],
                            'best_pair': best_opportunity['pair'],
                            'reason': final_reason
                        })

                        if success:
                            break
                        
                    else:
                        if reasons:
                            final_reason = Counter(reasons).most_common(1)[0][0]

                        best_pair = sorted(
                            unviable_opportunities, 
                            key=lambda x: x['funding_rate'], 
                            reverse=True
                        )[0]
                        
                        db_manager.log_scan_attempt({
                            'total_analyzed': len(top_pairs),
                            'passed_volume': len(top_pairs),
                            'best_funding': best_pair['funding_rate'],
                            'best_pair': best_pair['pair'],
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