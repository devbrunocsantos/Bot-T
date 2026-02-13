import json
import os
import ccxt
import time
import threading
import concurrent.futures
from datetime import datetime
from configs.config import *

class CashAndCarryBot:
    def __init__(self):
        """
        Inicializa o Bot.
        
        Args:
            initial_capital_usd (float): Capital inicial simulado.
            exchange_client (obj, optional): Cliente de exchange Mock para backtests. 
                                             Se None, conecta na Binance real via CCXT.
        """
        self.state_file = os.path.join("configs", "bot_state.json")

        # Dicionário base de configuração
        exchange_config = {
            'apiKey': API_KEY,
            'secret': API_SECRET,
            'enableRateLimit': True,
            'rateLimit': 200
        }

        # Inicializa cliente de Futuros (Swap)
        self.exchange_swap = getattr(ccxt, EXCHANGE_ID)({
            **exchange_config,  # Desempacota as credenciais
            'options': {'defaultType': 'swap'}
        })

        # Inicializa cliente Spot (À vista)
        self.exchange_spot = getattr(ccxt, EXCHANGE_ID)({
            **exchange_config,  # Desempacota as credenciais
            'options': {'defaultType': 'spot'}
        })

        # Inicialização de variáveis de estado
        if not self._load_state():
            current_real_balance = self.auto_balance_wallets()

            self.capital = current_real_balance
            self.position = None 
            self.accumulated_profit = 0.0
            self.accumulated_fees = 0.0
            self.fee_cache = {}
            self.peak_capital = current_real_balance
            self.last_real_balance = current_real_balance
            self.pending_deposit_usd = 0.0
            self.next_funding_timestamp = None
            self.last_usd_brl = BRL_USD_RATE

            self._save_state()

    def _save_state(self):
        """
        Persiste o estado financeiro e operacional em disco (JSON).
        """
        try:
            state = {
                'capital': self.capital,
                'position': self.position,
                'accumulated_profit': self.accumulated_profit,
                'accumulated_fees': self.accumulated_fees,
                'fee_cache': self.fee_cache,
                'peak_capital': self.peak_capital,
                'last_real_balance': self.last_real_balance,
                'pending_deposit_usd': self.pending_deposit_usd,
                'next_funding_timestamp': self.next_funding_timestamp,
                'last_usd_brl': self.last_usd_brl
            }
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=4)
        except Exception as e:
            LOGGER.error(f"Erro ao salvar estado: {e}")

    def _load_state(self):
        """
        Carrega o estado anterior se existir. Retorna True se sucesso.
        """
        if not os.path.exists(self.state_file):
            return False   
        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)  
            self.capital = state.get('capital', 0.0)
            self.position = state.get('position')
            self.accumulated_profit = state.get('accumulated_profit', 0.0)
            self.accumulated_fees = state.get('accumulated_fees', 0.0)
            self.fee_cache = state.get('fee_cache', {})
            self.peak_capital = state.get('peak_capital', 0.0)
            self.last_real_balance = state.get('last_real_balance', 0.0)
            self.pending_deposit_usd = state.get('pending_deposit_usd', 0.0)
            self.next_funding_timestamp = state.get('next_funding_timestamp')
            self.last_usd_brl = state.get('last_usd_brl', BRL_USD_RATE)
            
            LOGGER.info("Estado anterior carregado com SUCESSO.")
            return True
        except Exception as e:
            LOGGER.error(f"Erro ao carregar estado: {e}")
            return False
        
    def start_guardian(self):
        """
        Inicia a thread de proteção com uma CONEXÃO EXCLUSIVA.
        Isso evita conflitos de 'Nonce' e garante que o Guardião nunca seja bloqueado.
        """
        # Cria uma nova instância CCXT só para o Guardião (Clone das configs)
        guardian_config = {
            'apiKey': API_KEY,
            'secret': API_SECRET,
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'} # Foca em Futuros
        }
        
        # O atributo é novo: self.guardian_exchange
        self.guardian_exchange = getattr(ccxt, EXCHANGE_ID)(guardian_config)
        
        self.guardian_active = True
        
        LOGGER.info("Guardião: Conexão dedicada estabelecida.")
        
        guardian_thread = threading.Thread(target=self._guardian_loop, daemon=True)
        guardian_thread.start()

    def _guardian_loop(self):
        """
        Loop infinito que roda em background checando APENAS o risco de liquidação.
        """
        while self.guardian_active:
            # 1. Se não tem posição, descansa para economizar CPU e API
            if not self.position:
                time.sleep(5)
                continue

            # 2. Se tem posição, monitora com frequência alta (a cada 3s)
            try:
                symbol = self.position['symbol']
                
                # Busca apenas a posição específica (leve para a API)
                positions = self.guardian_exchange.fetch_positions([symbol])
                my_pos = next((p for p in positions if p['symbol'] == symbol), None)

                if my_pos:
                    liq_price = float(my_pos['liquidationPrice']) if my_pos['liquidationPrice'] else 0.0
                    mark_price = float(my_pos['markPrice'])
                    
                    if liq_price > 0:
                        # Cálculo da Distância para a Morte (Short: Liq > Mark)
                        distance_pct = (liq_price - mark_price) / mark_price

                        # Log de batimento cardíaco (opcional, bom para debug)
                        LOGGER.debug(f"Guardião: Distância Liq: {distance_pct:.2%}")

                        # 3. ZONA DE PERIGO (15% de distância)
                        if distance_pct < 0.15:
                            LOGGER.critical(f" >>>>> GUARDIÃO: RISCO CRÍTICO DETECTADO! Distância: {distance_pct:.2%} <<<<<")
                            LOGGER.critical(" >>>>> INICIANDO EJEÇÃO DE EMERGÊNCIA IMEDIATA <<<<<")
                            
                            # Dispara o fechamento na thread principal
                            spot_symbol = self.position['spot_symbol']
                            qty = self.position['size']
                            
                            # Fecha tudo
                            self.execute_real_close(symbol, spot_symbol, qty, reason="GUARDIAN_LIQUIDATION_RISK")
                            
                            # Pausa breve para evitar loop de ordens enquanto processa
                            time.sleep(10)
                            
            except Exception as e:
                # O Guardião não pode parar se der erro de rede, apenas loga e tenta de novo
                LOGGER.error(f"Erro no Guardião: {e}")
            
            # Frequência de Checagem: 3 segundos
            # É rápido o suficiente para evitar flash crash, mas não estoura o Rate Limit da Binance.
            time.sleep(3)
        
    def update_brl_rate(self, new_rate):
        """Atualiza a cotação USD/BRL e salva o estado."""
        self.last_usd_brl = new_rate
        self._save_state()

    # No arquivo strategy.py

    def get_top_volume_pairs(self):
        """
        Retorna um DICIONÁRIO {symbol: funding_rate} dos pares aprovados.
        Isso evita ter que buscar o funding de novo no main.py (Economiza API).
        """
        try:
            LOGGER.info("Iniciando varredura dinâmica de mercado...")
            # Busca Tickers de ambos os mercados
            tickers_swap = self.exchange_swap.fetch_tickers()
            tickers_spot = self.exchange_spot.fetch_tickers()

            available_spot_pairs = set(tickers_spot.keys())
            
            # Pré-filtro de volume
            self.exchange_swap.load_markets()
            self.exchange_spot.load_markets()

            candidates = []

            for symbol, data in tickers_swap.items():
                if '/USDT:USDT' in symbol:

                    spot_equivalent = symbol.split(':')[0]

                    if spot_equivalent in available_spot_pairs:
                        if data['quoteVolume'] >= MIN_24H_VOLUME_USD:

                            # Verificação segura de status
                            swap_market = self.exchange_swap.markets.get(symbol, {})
                            spot_market = self.exchange_spot.markets.get(spot_equivalent, {})

                            LOGGER.info(f"{COLOR_CYAN}Par Candidato: {symbol} | Swap: {swap_market.get('active', False)} | Spot: {spot_market.get('active', False)}{COLOR_RESET}")
                            is_active = swap_market.get('active', False) and spot_market.get('active', False)

                            if is_active:
                                candidates.append(symbol)

            top_candidates = sorted(candidates, key=lambda x: tickers_swap[x]['quoteVolume'], reverse=True)[:25]
            
            valid_pairs_data = {} 

            for symbol in top_candidates:
                # O filtro agora retorna (Bool, Rate)
                is_valid, rate, avg_rate = self._analyze_funding_consistency(symbol)
                time.sleep(1)  # Pequena pausa para evitar sobrecarga de API
                
                if is_valid:
                    valid_pairs_data[symbol] = rate
                    LOGGER.info(f"{COLOR_GREEN}[APROVADO]: {symbol} | Funding Atual: {rate:.4%} | Funding Médio: {avg_rate:.4%}{COLOR_RESET}")
                else:
                    if avg_rate > 0:
                        LOGGER.debug(f"{COLOR_RED}[REJEITADO]: {symbol} | Funding Atual: {rate:.4%} | Funding Médio: {avg_rate:.4%}{COLOR_RESET}")
            
            return valid_pairs_data, tickers_swap, tickers_spot
            
        except Exception as e:
            LOGGER.error(f"Erro no scanner: {e}")
            return {}

    def _analyze_funding_consistency(self, symbol):
        """
        Analisa o histórico e retorna o Funding Rate atual validado.
        Retorno: (True/False, current_rate)
        """
        try:
            # Busca histórico
            history = self.exchange_swap.fetch_funding_rate_history(symbol, limit=20)
            
            if not history or len(history) < 9: 
                return False, 0.0, 0.0
            
            recent_rates = [entry['fundingRate'] for entry in history[-9:]]
            
            # 1. Média Atrativa
            avg_rate = sum(recent_rates) / len(recent_rates)
            if avg_rate < 0.0001: 
                return False, 0.0, 0.0

            # 2. Momento Atual Positivo
            current_rate = recent_rates[-1]
            if current_rate < 0:
                return False, 0.0, 0.0
            
            return True, current_rate, avg_rate
            
        except: 
            return False, 0.0, 0.0

    def check_entry_opportunity(self, symbol, spot_symbol, price_spot, price_swap, funding_rate):
        """
        Avalia viabilidade de entrada.
        Args:
            price_spot (float): Preço atual do Spot.
            price_swap (float): Preço atual do Futuro.
            funding_rate (float): Taxa de funding atual.
        """
        try:
            # 1. Taxas
            real_fee_spot = self._get_real_fee_rate(spot_symbol, swap=False)
            real_fee_swap = self._get_real_fee_rate(symbol, swap=True)
            
            # Define margem de segurança para taxas (1.1 = 10% de buffer sobre a taxa)
            estimated_fee_pct = (real_fee_spot + real_fee_swap) * 1.1
            
            # Reduz o capital base para garantir que sobra dinheiro para as taxas
            usable_capital = self.capital / (1 + estimated_fee_pct)
            
            allocation_per_leg = usable_capital / 2

            # 2. Slippage Real (Impacto de Mercado)
            # Slippage da Perna Spot (Compra)
            slippage_spot = self._calculate_market_impact(spot_symbol, allocation_per_leg, side='buy', swap=False)
            # Slippage da Perna Futura (Venda/Short)
            slippage_swap = self._calculate_market_impact(symbol, allocation_per_leg, side='sell', swap=True)

            total_custo_spot = (real_fee_spot * 2) + (slippage_spot * 2)
            total_custo_swap = (real_fee_swap * 2) + (slippage_swap * 2)
            
            total_fees_real = total_custo_spot + total_custo_swap

            funding_frequency_daily = 3.0 # Fallback padrão (8h)
            
            try:
                # Carrega dados cacheados do mercado pelo CCXT
                market = self.exchange_swap.market(symbol)
                
                # Verifica se existe informação específica de intervalo (comum na Binance: fundingIntervalHours)
                if 'info' in market and 'fundingIntervalHours' in market['info']:
                    interval_hours = int(market['info']['fundingIntervalHours'])
                    if interval_hours > 0:
                        funding_frequency_daily = 24 / interval_hours
            except Exception as e:
                # Mantém o fallback silenciosamente em caso de erro de lookup, mas loga se necessário
                LOGGER.debug(f"Não foi possível obter intervalo dinâmico para {symbol}, usando 8h: {e}")
                pass

            # Fórmula: Meta Anual / 365 * Dias
            required_net_profit = (MIN_NET_APR / 365) * PAYBACK_PERIOD_DAYS
            
            # O retorno tem que pagar as Taxas + O Lucro Mínimo
            hurdle_rate = total_fees_real + required_net_profit

            # Projeção do Funding Real
            projected_return = (funding_rate * funding_frequency_daily) * PAYBACK_PERIOD_DAYS

            LOGGER.info(f"Projeção de Funding: {symbol} | {projected_return}")
            LOGGER.info(f"Funding para 0.15: {hurdle_rate}")

            if projected_return < hurdle_rate:
                return False, funding_rate, "LOW_PROFIT_VS_FEES"

            # 3. Verificação de Basis (Usando os preços recebidos)
            basis_percent = (price_swap - price_spot) / price_spot

            if basis_percent < NEGATIVE_FUNDING_THRESHOLD: 
                return False, funding_rate, f"BACKWARDATION ({basis_percent:.4%})"

            return True, funding_rate, "SUCCESS"

        except Exception as e:
            LOGGER.error(f"Erro ao verificar oportunidade para {symbol}: {e}")
            return False, 0.0, f"ERROR"
        
    def execute_real_entry(self, symbol, spot_symbol, allocation_usd):
        """
        Executa entrada simultânea (Spot + Swap) com proteção de Rollback.
        Usa Threading para disparar as ordens no mesmo milissegundo.
        """
        LOGGER.info(f"--- INICIANDO EXECUÇÃO REAL: {symbol} ---")
        
        # 1. Preparação de Dados e Preços
        try:
            # Baixa preços atualizados para calcular limites
            ticker_spot = self.exchange_spot.fetch_ticker(spot_symbol)
            ticker_swap = self.exchange_swap.fetch_ticker(symbol)
            
            price_spot = ticker_spot['last']
            price_swap = ticker_swap['last']
            
            # Tolerância de Slippage (0.5%)
            limit_buy_price = price_spot * 1.005
            limit_sell_price = price_swap * 0.995

            real_fee_spot = self._get_real_fee_rate(spot_symbol, swap=False)
            real_fee_swap = self._get_real_fee_rate(symbol, swap=True)

            estimated_fee_pct = (real_fee_spot + real_fee_swap) * 1.1
            
            # Cálculo do capital útil descontando taxas previstas
            usable_capital = allocation_usd / (1 + estimated_fee_pct)
            
            # Calcula quantidades baseadas no capital alocado
            raw_amount = (usable_capital / 2) / limit_buy_price
            
            amount_spot = self.exchange_spot.amount_to_precision(spot_symbol, raw_amount)
            amount_swap = self.exchange_swap.amount_to_precision(symbol, raw_amount)
            
            # Formata preços para precisão da exchange
            price_spot_fmt = self.exchange_spot.price_to_precision(spot_symbol, limit_buy_price)
            price_swap_fmt = self.exchange_swap.price_to_precision(symbol, limit_sell_price)

            LOGGER.info(f"Tentativa: Comprar {amount_spot} {spot_symbol} @ {price_spot_fmt} | Short {amount_swap} {symbol} @ {price_swap_fmt}")

        except Exception as e:
            LOGGER.error(f"Erro na preparação da ordem real: {e}")
            return False

        # 2. Execução Paralela (Disparo Simultâneo)
        # Usamos ThreadPool para não travar o código esperando uma resposta antes de enviar a outra
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            
            # Prepara as "balas"
            future_spot = executor.submit(
                self._place_limit_ioc_order, 
                self.exchange_spot, 
                spot_symbol, 
                'buy', 
                amount_spot, 
                price_spot_fmt
            )
            
            future_swap = executor.submit(
                self._place_limit_ioc_order, 
                self.exchange_swap, 
                symbol, 
                'sell', 
                amount_swap, 
                price_swap_fmt
            )
            
            # Espera os resultados
            order_spot = future_spot.result()
            order_swap = future_swap.result()

        # 3. Verificação de Sucesso e Lógica de Rollback
        spot_ok = order_spot is not None and order_spot['status'] in ['filled', 'closed']
        swap_ok = order_swap is not None and order_swap['status'] in ['filled', 'closed']
        
        # CENÁRIO A: SUCESSO TOTAL
        if spot_ok and swap_ok:
            LOGGER.info(f"SUCESSO TOTAL! Ordens executadas. Spot ID: {order_spot['id']} | Swap ID: {order_swap['id']}")
            
            # Atualiza estado interno do bot com dados reais da exchange
            self.position = {
                'symbol': symbol,
                'spot_symbol': spot_symbol,
                'size': float(order_swap['filled']), # Usa o que foi realmente preenchido
                'entry_price_spot': float(order_spot['average']),
                'entry_price_swap': float(order_swap['average']),
                'entry_time': time.time()
            }
            self._save_state()
            return True

        # CENÁRIO B: FALHA PARCIAL (PERIGO!) -> ROLLBACK
        else:
            LOGGER.critical("FALHA NA EXECUÇÃO SIMULTÂNEA! Iniciando Protocolo de Rollback...")
            
            # Se comprou Spot mas falhou no Futuro -> Vende o Spot a mercado
            if spot_ok and not swap_ok:
                LOGGER.warning("Rollback: Vendendo Spot comprado incorretamente...")
                try:
                    self.exchange_spot.create_market_sell_order(spot_symbol, order_spot['filled'])
                    LOGGER.info("Rollback Spot concluído.")
                except Exception as e:
                    LOGGER.critical(f"FALHA GRAVE NO ROLLBACK SPOT: {e}")

            # Se vendeu Futuro mas falhou no Spot -> Fecha o Futuro a mercado
            elif swap_ok and not spot_ok:
                LOGGER.warning("Rollback: Fechando Short aberto incorretamente...")
                try:
                    self.exchange_swap.create_market_buy_order(symbol, order_swap['filled'])
                    LOGGER.info("Rollback Swap concluído.")
                except Exception as e:
                    LOGGER.critical(f"FALHA GRAVE NO ROLLBACK SWAP: {e}")
            
            return False

    def monitor_and_manage(self, db_manager):
        if not self.position: return

        now = time.time()
        symbol = self.position['symbol']
        spot_symbol = self.position['spot_symbol']
        
        try:
            # 1. Busca dados do Futuro (Necessário para PnL e Monitoramento)
            ticker_swap = self.exchange_swap.fetch_ticker(symbol)
            price_swap = ticker_swap['last']

            try:
                # Tenta buscar o preço real do ativo no mercado à vista
                ticker_spot = self.exchange_spot.fetch_ticker(spot_symbol)
                price_spot = ticker_spot['last']
            except Exception as e:
                # Em caso de falha na API Spot, mantém o fallback e loga aviso
                LOGGER.warning(f"Falha ao buscar preço Spot para monitoramento: {e}. Usando proxy.")
                price_spot = price_swap
            
            # --- Lógica de Funding (Inalterada) ---
            funding_info = self.exchange_swap.fetch_funding_rate(symbol)
            current_funding = funding_info['fundingRate']
            
            api_next_funding_ts = funding_info.get('nextFundingTimestamp')
            api_next_funding_sec = api_next_funding_ts / 1000 if api_next_funding_ts else None

            if self.next_funding_timestamp and now >= self.next_funding_timestamp:
                funding_payout = (self.position['size'] * price_swap) * current_funding
                self.accumulated_profit += funding_payout
                
                if api_next_funding_sec and api_next_funding_sec > now:
                    self.next_funding_timestamp = api_next_funding_sec

            # --- Circuit Breaker ---
            if current_funding < NEGATIVE_FUNDING_THRESHOLD:
                LOGGER.warning(f"SAIDA FORÇADA: Funding negativo crítico ({current_funding:.4%})")
                self.execute_real_close(symbol, spot_symbol, self.position['size'], "CIRCUIT_BREAKER")
                return

            # --- Cálculo de PnL Flutuante ---
            spot_pnl = (price_spot - self.position['entry_price_spot']) * self.position['size']
            swap_pnl = (self.position['entry_price_swap'] - price_swap) * self.position['size']
            net_pnl_price = spot_pnl + swap_pnl
            
            total_equity = self.capital + self.accumulated_profit + net_pnl_price
            
            if total_equity > self.peak_capital:
                self.peak_capital = total_equity
            
            drawdown = (self.peak_capital - total_equity) / self.peak_capital if self.peak_capital > 0 else 0

            try:
                self.auto_balance_wallets()
            except Exception as e:
                LOGGER.error(f"Falha no auto-balanceamento durante monitoramento: {e}")

            # Se passou nos filtros, executa o aumento de posição
            if self.pending_deposit_usd >= MIN_ORDER_VALUE_USD:
                # --- Lógica de Reinvestimento Condicional ---
                self._process_compounding(symbol, spot_symbol, price_spot, price_swap)

            # --- Logging ---
            log_data = {
                'symbol': symbol,
                'price_swap': price_swap,
                'funding_rate': current_funding,
                'next_funding_time': datetime.fromtimestamp(self.next_funding_timestamp).strftime('%Y-%m-%d %H:%M:%S'),
                'position_size': self.position['size'],
                'simulated_fees': self.accumulated_fees,
                'accumulated_profit': self.accumulated_profit + net_pnl_price,
                'max_drawdown': drawdown,
                'action': 'HOLD'
            }
            
            if hasattr(db_manager, 'log_state'):
                db_manager.log_state(log_data)
            
            self._save_state()

        except Exception as e:
            LOGGER.error(f"Monitor error: {e}")

    def execute_real_close(self, symbol, spot_symbol, quantity, reason="SIGNAL"):
        """
        Encerra a posição (Vende Spot + Compra Futuro) simultaneamente.
        Usa 'Limit IOC' com slippage generoso para garantir a saída, 
        mas força 'Market' se algo der errado.
        """
        LOGGER.info(f"--- INICIANDO FECHAMENTO REAL: {symbol} (Motivo: {reason}) ---")
        
        try:
            # 1. Preparação de Dados
            ticker_spot = self.exchange_spot.fetch_ticker(spot_symbol)
            ticker_swap = self.exchange_swap.fetch_ticker(symbol)
            
            price_spot = ticker_spot['last']
            price_swap = ticker_swap['last']
            
            # Tolerância de Slippage na SAÍDA (0.5%)
            limit_sell_spot = price_spot * 0.995
            
            limit_buy_swap = price_swap * 1.005
            
            # Ajuste de precisão (Quantidade)
            qty_spot = self.exchange_spot.amount_to_precision(spot_symbol, quantity)
            qty_swap = self.exchange_swap.amount_to_precision(symbol, quantity)
            
            # Ajuste de precisão (Preço)
            price_spot_fmt = self.exchange_spot.price_to_precision(spot_symbol, limit_sell_spot)
            price_swap_fmt = self.exchange_swap.price_to_precision(symbol, limit_buy_swap)

            LOGGER.info(f"Fechando: Vender Spot {qty_spot} @ {price_spot_fmt} | Comprar Swap {qty_swap} @ {price_swap_fmt}")

            # 2. Execução Paralela
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                # Dispara Venda do Spot
                future_spot = executor.submit(
                    self._place_limit_ioc_order, 
                    self.exchange_spot, spot_symbol, 'sell', qty_spot, price_spot_fmt
                )
                
                # Dispara Compra do Swap (Fechar Short)
                future_swap = executor.submit(
                    self._place_limit_ioc_order, 
                    self.exchange_swap, symbol, 'buy', qty_swap, price_swap_fmt
                )
                
                order_spot = future_spot.result()
                order_swap = future_swap.result()

            # 3. Verificação e "Force Close" (Limpeza de Erros)
            spot_done = order_spot is not None and order_spot['status'] in ['filled', 'closed']
            swap_done = order_swap is not None and order_swap['status'] in ['filled', 'closed']

            # CASO PERFEITO: Ambos saíram
            if spot_done and swap_done:
                LOGGER.info("POSIÇÃO ENCERRADA COM SUCESSO NO MODO REAL.")
                self._clean_spot_dust(spot_symbol)
                self.position = None
                self._save_state()
                return True

            # CASO DE ERRO:Força saída a Mercado
            else:
                LOGGER.critical("ERRO NO FECHAMENTO SIMULTÂNEO! Iniciando Saída de Emergência (Market Order)...")
                
                # Se Spot não vendeu, vende a mercado agora
                if not spot_done:
                    try:
                        LOGGER.warning("Forçando Venda de Spot a Mercado...")
                        self.exchange_spot.create_market_sell_order(spot_symbol, qty_spot)
                    except Exception as e:
                        LOGGER.critical(f"FALHA CRÍTICA AO VENDER SPOT: {e}")

                # Se Swap não fechou, compra a mercado agora
                if not swap_done:
                    try:
                        LOGGER.warning("Forçando Fechamento de Swap a Mercado...")
                        self.exchange_swap.create_market_buy_order(symbol, qty_swap)
                    except Exception as e:
                        LOGGER.critical(f"FALHA CRÍTICA AO FECHAR SWAP: {e}")
                
                # Assume que limpou tudo após a emergência
                self.position = None
                self._save_state()
                return True

        except Exception as e:
            LOGGER.error(f"Erro catastrófico no fechamento real: {e}")
            return False

    def _process_compounding(self, symbol, spot_symbol, price_spot, price_swap):
        """
        Aumenta a posição se houver saldo pendente, executando ordens REAIS na exchange.
        Inclui proteções de slippage e precisão de ativos.
        """
        try:
            # Busca o Funding Rate atualizado antes de gastar taxas
            funding_info = self.exchange_swap.fetch_funding_rate(symbol)
            current_funding = funding_info['fundingRate']
        except Exception as e:
            LOGGER.warning(f"Reinvestimento abortado: Falha ao checar funding atual ({e})")
            return

        # Validação de Basis (Spread de Preço)
        current_basis = (price_swap - price_spot) / price_spot
        
        # Se o spread estiver comprimido (< 0.05%), não vale a pena pagar taxas de Taker
        if current_basis < 0.0005: 
            LOGGER.info(f"Reinvestimento adiado. Basis comprimido: {current_basis:.4%}")
            return

        # Validação de Rentabilidade
        if current_funding < MIN_FUNDING_RATE:
            LOGGER.info(f"Reinvestimento adiado. Funding baixo: {current_funding:.4%}")
            return

        
            
        LOGGER.info(f"--- INICIANDO REINVESTIMENTO REAL: ${self.pending_deposit_usd:.2f} ---")

        real_fee_spot = self._get_real_fee_rate(spot_symbol, swap=False)
        real_fee_swap = self._get_real_fee_rate(symbol, swap=True)

        estimated_fee_pct = (real_fee_spot + real_fee_swap) * 1.1
        
        # Cálculo do capital útil descontando taxas previstas
        usable_capital = self.pending_deposit_usd / (1 + estimated_fee_pct)
        allocation_per_leg = usable_capital / 2

        # --- 1. Preparação dos Parâmetros de Ordem (Precisão e Slippage) ---
        try:
            # Slippage
            slippage_spot = self._calculate_market_impact(spot_symbol, allocation_per_leg, side='buy', swap=False)
            slippage_swap = self._calculate_market_impact(symbol, allocation_per_leg, side='sell', swap=True)

            # Preços Limite (com margem para garantir execução IOC)
            limit_buy_price = price_spot * (1 + slippage_spot)
            limit_sell_price = price_swap * (1 - slippage_swap)

            # Cálculo da quantidade bruta
            raw_amount = allocation_per_leg / limit_buy_price

            # Ajuste de Precisão para a Exchange (Ex: 0.00123 BTC)
            amount_spot = self.exchange_spot.amount_to_precision(spot_symbol, raw_amount)
            amount_swap = self.exchange_swap.amount_to_precision(symbol, raw_amount)
            
            # Ajuste de Precisão de Preço
            price_spot_fmt = self.exchange_spot.price_to_precision(spot_symbol, limit_buy_price)
            price_swap_fmt = self.exchange_swap.price_to_precision(symbol, limit_sell_price)

        except Exception as e:
            LOGGER.error(f"Erro na preparação do reinvestimento: {e}")
            return

        # --- 2. Execução Paralela (Spot Buy + Swap Sell) ---
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_spot = executor.submit(
                self._place_limit_ioc_order, 
                self.exchange_spot, spot_symbol, 'buy', amount_spot, price_spot_fmt
            )
            
            future_swap = executor.submit(
                self._place_limit_ioc_order, 
                self.exchange_swap, symbol, 'sell', amount_swap, price_swap_fmt
            )
            
            order_spot = future_spot.result()
            order_swap = future_swap.result()

        # --- 3. Verificação e Atualização de Estado ---
        spot_ok = order_spot is not None and order_spot['status'] in ['filled', 'closed']
        swap_ok = order_swap is not None and order_swap['status'] in ['filled', 'closed']

        if spot_ok and swap_ok:
            # Recupera dados executados reais da exchange
            filled_qty = float(order_swap['filled'])
            exec_price_spot = float(order_spot['average'])
            exec_price_swap = float(order_swap['average'])

            # Cálculo de Taxas Reais Pagas
            cost_spot = (filled_qty * exec_price_spot) * real_fee_spot
            cost_swap = (filled_qty * exec_price_swap) * real_fee_swap
            actual_fees = cost_spot + cost_swap

            # Dados Antigos para Ponderação
            old_qty = self.position['size']
            old_price_spot = self.position['entry_price_spot']
            old_price_swap = self.position['entry_price_swap']
            
            total_new_qty = old_qty + filled_qty

            # Cálculo do Novo Preço Médio (Weighted Average)
            avg_price_spot = ((old_price_spot * old_qty) + (exec_price_spot * filled_qty)) / total_new_qty
            avg_price_swap = ((old_price_swap * old_qty) + (exec_price_swap * filled_qty)) / total_new_qty

            # Atualização do Estado
            self.position['size'] = total_new_qty
            self.position['entry_price_spot'] = avg_price_spot
            self.position['entry_price_swap'] = avg_price_swap
            
            # Atualização Financeira
            self.capital += self.pending_deposit_usd # Incorpora o depósito ao capital do bot
            self.capital -= actual_fees              # Desconta as taxas pagas
            self.accumulated_fees += actual_fees
            self.pending_deposit_usd = 0.0           # Zera o pendente
            
            LOGGER.info(f"REINVESTIMENTO SUCESSO: +{filled_qty} moedas. Novo PM Spot: {avg_price_spot:.4f}")
            self._save_state()

        else:
            # --- Lógica de Rollback (Segurança) ---
            LOGGER.critical("FALHA PARCIAL NO REINVESTIMENTO! Revertendo...")
            
            # Se comprou Spot mas falhou Swap -> Vende Spot
            if spot_ok and not swap_ok:
                try:
                    self.exchange_spot.create_market_sell_order(spot_symbol, order_spot['filled'])
                    LOGGER.info("Rollback: Spot extra vendido.")
                except Exception as e:
                    LOGGER.critical(f"ERRO ROLLBACK SPOT: {e}")

            # Se vendeu Swap mas falhou Spot -> Fecha Swap
            elif swap_ok and not spot_ok:
                try:
                    self.exchange_swap.create_market_buy_order(symbol, order_swap['filled'])
                    LOGGER.info("Rollback: Short extra fechado.")
                except Exception as e:
                    LOGGER.critical(f"ERRO ROLLBACK SWAP: {e}")

    def _get_real_fee_rate(self, symbol, swap=False):
        """
        Busca a taxa de Taker real da conta via API.
        """
        try:
            # Seleciona o cliente correto (Spot ou Swap) e o valor padrão
            if swap:
                client = self.exchange_swap
                default_fee = FEE_TAKER_SWAP_DEFAULT
                market_type = 'swap'
            else:
                client = self.exchange_spot
                default_fee = FEE_TAKER_SPOT_DEFAULT
                market_type = 'spot'

            # Cache simples para evitar spam na API (opcional: limpar a cada X horas)
            cache_key = f"fee_{market_type}_{symbol}"
            if hasattr(self, 'fee_cache') and cache_key in self.fee_cache:
                return self.fee_cache[cache_key]

            # Busca na API
            fees = client.fetch_trading_fees()
            
            # Tenta pegar a taxa específica do par, ou o padrão 'USDT'
            # A estrutura do retorno pode variar, mas geralmente é fees['BTC/USDT']['taker']
            if symbol in fees:
                taker_fee = fees[symbol]['taker']
            else:
                # Fallback genérico da resposta da API
                taker_fee = fees.get('USDT', {}).get('taker', default_fee)

            # Salva no cache
            self.fee_cache[cache_key] = taker_fee
            
            return taker_fee

        except Exception as e:
            LOGGER.warning(f"Erro ao buscar fee real ({symbol}): {e}. Usando default.")
            return FEE_TAKER_SWAP_DEFAULT if swap else FEE_TAKER_SPOT_DEFAULT
        
    def _place_limit_ioc_order(self, client, symbol, side, amount, limit_price):
        """
        Envia uma ordem LIMIT com TimeInForce = IOC (Immediate-Or-Cancel).
        Isso simula uma ordem a mercado, mas com proteção de preço (Slippage máximo).
        """
        try:
            # params={'timeInForce': 'IOC'} instrui a Binance a cancelar imediatamente
            # qualquer parte da ordem que não possa ser preenchida ao preço limite ou melhor.
            order = client.create_order(
                symbol=symbol,
                type='limit',
                side=side,
                amount=amount,
                price=limit_price,
                params={'timeInForce': 'IOC'} 
            )
            return order
        except Exception as e:
            LOGGER.error(f"Falha na execução da perna {side} ({symbol}): {e}")
            return None

    def _calculate_market_impact(self, symbol, usd_amount, side='buy', swap=False):
        """
        Calcula o Slippage real simulando uma ordem a mercado no Order Book atual.
        
        Args:
            symbol: Par a ser negociado.
            usd_amount: Valor financeiro da ordem em USD.
            side: 'buy' (olha os asks) ou 'sell' (olha os bids).
        """
        try:
            # Busca as 50 melhores ofertas do livro
            limit = 50

            if swap:
                order_book = self.exchange_swap.fetch_order_book(symbol, limit=limit)
            else:
                order_book = self.exchange_spot.fetch_order_book(symbol, limit=limit)
            
            # Se quero COMPRAR, consumo quem está VENDENDO (asks)
            # Se quero VENDER, consumo quem está COMPRANDO (bids)
            book = order_book['asks'] if side == 'buy' else order_book['bids']
            
            if not book: return SLIPPAGE_SIMULATED

            amount_filled = 0.0
            total_cost = 0.0
            
            # Preço inicial (topo do livro) para referência
            best_price = book[0][0]
            
            # Simula a execução da ordem nível a nível
            for price, qty in book:
                # Quanto custa levar esse nível inteiro?
                level_value = price * qty
                
                remaining = usd_amount - total_cost
                
                if level_value >= remaining:
                    # Preenche o resto aqui e termina
                    qty_needed = remaining / price
                    total_cost += remaining
                    amount_filled += qty_needed
                    break
                else:
                    # Consome o nível todo e continua
                    total_cost += level_value
                    amount_filled += qty
            
            if amount_filled == 0: return SLIPPAGE_SIMULATED

            # Preço Médio Final = Custo Total / Quantidade de Moedas
            avg_executed_price = total_cost / amount_filled
            
            # Cálculo do Slippage Percentual
            # Buy: Paguei mais caro que o topo? (Avg > Best)
            # Sell: Vendi mais barato que o topo? (Avg < Best)
            slippage_pct = abs(avg_executed_price - best_price) / best_price
            
            return slippage_pct

        except Exception as e:
            LOGGER.warning(f"Erro ao calcular slippage real para {symbol}: {e}")
            return SLIPPAGE_SIMULATED
        
    def auto_balance_wallets(self, threshold_usd=1.0):
        """
        Gerencia o equilíbrio entre carteiras com segurança.
        
        1. Calcula o saldo total REAL (Spot + Futuros).
        2. Se estiver SEM POSIÇÃO: Rebalanceia 50/50 e detecta aportes pelo saldo total.
        3. Se estiver COM POSIÇÃO: Ignora saldo total (para não contar PnL) e detecta aportes apenas no Spot.
        """
        try:
            # 1. Busca Saldo Livre Real (Free Balance)
            bal_spot_raw = self.exchange_spot.fetch_balance()
            free_spot = bal_spot_raw.get('USDT', {}).get('free', 0.0)

            time.sleep(1)

            bal_swap_raw = self.exchange_swap.fetch_balance()
            free_swap = bal_swap_raw.get('USDT', {}).get('free', 0.0)

            current_total_real = free_spot + free_swap

            # Se self.position não existir ainda, assume None
            current_position = getattr(self, 'position', None)
            
            # Se self.last_real_balance não existir, assume 0.0
            last_balance = getattr(self, 'last_real_balance', 0.0)

            # --- CENÁRIO A: Bot Líquido (Sem Posição) ---
            if current_position is None:
                
                # Lógica de Detecção de Aporte (Baseada no Total)
                if last_balance > 0:
                    balance_diff = current_total_real - last_balance
                    
                    if balance_diff > 1.0:
                        LOGGER.info(f"Saldo total aumentou em ${balance_diff:.2f} (Aporte Detectado)")
                        # Pega o valor atual de pending_deposit (ou 0 se não existir)
                        current_pending = getattr(self, 'pending_deposit_usd', 0.0)
                        
                        # Soma e atribui
                        self.pending_deposit_usd = current_pending + balance_diff
                        
                        # Só salva se o método _save_state já estiver pronto (segurança extra)
                        if hasattr(self, '_save_state'):
                            self._save_state()

                # Atualiza a referência do último saldo conhecido
                self.last_real_balance = current_total_real
                
                # Lógica de Transferência 50/50
                target_per_wallet = current_total_real / 2
                diff = free_spot - target_per_wallet

                if diff > threshold_usd:
                    self.exchange_spot.transfer('USDT', diff, 'spot', 'future')
                    LOGGER.info(f"Balanceamento: Spot -> Futuros (${diff:.2f})")
                elif diff < -threshold_usd:
                    amount = abs(diff)
                    self.exchange_spot.transfer('USDT', amount, 'future', 'spot')
                    LOGGER.info(f"Balanceamento: Futuros -> Spot (${amount:.2f})")
                
                return current_total_real

            # --- CENÁRIO B: Bot Posicionado (Trade Aberto) ---
            else:                
                # Se tem dinheiro livre no Spot (> $5), assumimos que é dinheiro novo (Aporte)
                if free_spot > 5.0:
                    amount_to_transfer = free_spot / 2
                    
                    LOGGER.info(f"APORTE DETECTADO COM POSIÇÃO ABERTA! Spot Livre: ${free_spot:.2f}")
                    LOGGER.info(f"Enviando ${amount_to_transfer:.2f} para margem...")

                    self.exchange_spot.transfer('USDT', amount_to_transfer, 'spot', 'future')
                    
                    # Atualiza pendente com segurança
                    current_pending = getattr(self, 'pending_deposit_usd', 0.0)
                    self.pending_deposit_usd = current_pending + free_spot
                    
                    if hasattr(self, '_save_state'):
                        self._save_state()
                
                return current_total_real

        except Exception as e:
            LOGGER.error(f"Erro no balanceamento: {e}")
            # Em caso de erro, retorna o que tiver na memória ou 0.0 para não travar
            return getattr(self, 'capital', 0.0)
        
    def _clean_spot_dust(self, spot_symbol):
        """
        Verifica se restou saldo residual (dust) na carteira Spot e tenta vender a mercado.
        Nota: A ordem só será aceita se o valor da sobra for maior que o mínimo da exchange (ex: > $5 USD na Binance).
        """
        try:
            base_currency = spot_symbol.split('/')[0] # Ex: 'BTC/USDT' -> 'BTC'
            
            # Busca saldo atualizado especificamente da moeda base
            balance_raw = self.exchange_spot.fetch_balance()
            free_amount = balance_raw.get(base_currency, {}).get('free', 0.0)

            if free_amount <= 0:
                return

            LOGGER.info(f"Detectada sobra de {free_amount} {base_currency}. Tentando limpar...")

            # Tenta vender tudo o que sobrou a mercado
            self.exchange_spot.create_market_sell_order(spot_symbol, free_amount)
            
            LOGGER.info(f"Limpeza de dust realizada: {free_amount} {base_currency} vendidos.")

        except Exception as e:
            # Erros de 'Min Notional' (valor muito baixo) são esperados e podem ser ignorados ou logados como aviso leve
            if "MIN_NOTIONAL" in str(e) or "Filter failure" in str(e):
                LOGGER.info(f"Sobra muito pequena para vender ({base_currency}). Mantida na carteira.")
            else:
                LOGGER.warning(f"Não foi possível limpar a sobra de {spot_symbol}: {e}")
