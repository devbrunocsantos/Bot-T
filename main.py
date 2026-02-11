import os
import re
import time
from datetime import datetime
from collections import Counter
from difflib import SequenceMatcher
import requests
from configs.config import LOGGER, BRL_USD_RATE
from tools.database import DataManager
from tools.strategy import CashAndCarryBot
from utils import configurar_ambiente_proxy

# --- Configurações de Proxy do Usuário ---
proxy_user = "ter.brunokawan"
proxy_pass = "Kawan72643233"
proxy_host = "10.15.54.113"
proxy_porta = 8080

configurar_ambiente_proxy(proxy_host=proxy_host, proxy_porta=proxy_porta, proxy_user=proxy_user, proxy_pass=proxy_pass)

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
    LOGGER.info("Iniciando Bot Cash & Carry (Modo Simulado)...")
    
    # Inicialização do Bot
    bot = CashAndCarryBot(initial_capital_usd=1000.0) 
    
    # Variáveis de controle de tempo
    last_scan_time = 0
    scan_interval = 3600 # 1 hora
    last_deposit_check = time.time()
    deposit_interval = 2592000 # 30 dias (Simulação simplificada)

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

            # 1. Simulação de Aporte Mensal
            if current_time - last_deposit_check > deposit_interval:
                current_rate = get_live_usd_brl(bot)
                bot.deposit_monthly_contribution(exchange_rate=current_rate)
                last_deposit_check = current_time

            # 2. Lógica de Mercado
            if bot.position is None:
                # Se não tem posição, escaneia
                if current_time - last_scan_time > scan_interval:
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
                            # Retorna chaves como 'BTC/USDT'
                            # NOTA: Requer que bot.exchange_spot tenha sido criado no strategy.py
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

                            # 1. Definições Iniciais
                            # pair futura ex: 'POWER/USDT:USDT'
                            symbol_spot_candidate = pair.split(':')[0] 
                            base_swap_raw = pair.split('/')[0] # 'POWER'
                            
                            # Limpeza inteligente de prefixos numéricos (1000PEPE -> PEPE)
                            base_swap_clean = re.sub(r"^\d+", "", base_swap_raw)

                            found_spot = None
                            
                            # ESTRATÉGIA A: Busca Direta (A mais segura e rápida)
                            if symbol_spot_candidate in tickers_spot:
                                found_spot = symbol_spot_candidate
                            
                            # ESTRATÉGIA B: Busca Heurística (Se a direta falhou)
                            # Só entra aqui se não achou 'POWER/USDT' direto
                            else:
                                best_match_score = 0
                                best_match_symbol = None

                                for s_symbol, s_data in tickers_spot.items():
                                    # Filtra apenas pares USDT e ignora preços zerados
                                    if not s_symbol.endswith('/USDT') or s_data['last'] == 0:
                                        continue
                                    
                                    base_spot = s_symbol.split('/')[0] # ex: 'POWR'

                                    # [NOVO] MARCADOR 1: Similaridade de Texto (Levenshtein)
                                    # Compara 'PEPE' (swap limpo) com 'PEPE' (spot) -> 1.0 (100%)
                                    # Compara 'POWER' (swap) com 'POWR' (spot) -> 0.88 (88%)
                                    # Compara 'USDC' com 'USDT' -> 0.75 (75%)
                                    similarity = SequenceMatcher(None, base_swap_clean, base_spot).ratio()
                                    
                                    # Só consideramos candidatos com alta similaridade textual (>80%)
                                    if similarity < 0.80:
                                        continue

                                    # [NOVO] MARCADOR 2: Validação de Preço (O "Tira-Teima")
                                    # Se o texto é parecido, o preço TEM que ser quase idêntico.
                                    price_swap = tickers_swap[pair]['last']
                                    price_spot = s_data['last']
                                    price_diff = abs(price_swap - price_spot) / price_spot
                                    
                                    # Se a diferença for maior que 1.5%, rejeita (evita tokens v1/v2 ou scams)
                                    if price_diff > 0.015:
                                        continue
                                    
                                    # Se passou nos dois testes e é o "melhor" até agora, guarda
                                    if similarity > best_match_score:
                                        best_match_score = similarity
                                        best_match_symbol = s_symbol

                                if best_match_symbol:
                                    found_spot = best_match_symbol
                                    # Log de auditoria para você ver a "mágica" acontecendo
                                    LOGGER.info(f"Link Inteligente: {pair} <-> {found_spot} (Score Texto: {best_match_score:.2f})")

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
                            # Executa entrada (ainda faz fetch interno para precisão de ordem)
                            success = bot.simulate_entry(pair, found_spot, fr)
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

                    last_scan_time = current_time
            else:
                # Se tem posição, monitora
                bot.monitor_and_manage(db_manager)

            # Aguarda próximo ciclo
            time.sleep(60) 

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