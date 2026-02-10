import json
import os
import random
import ccxt
import time
import urllib3
from datetime import datetime
from configs.config import *

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class CashAndCarryBot:
    def __init__(self, initial_capital_usd):
        proxies = None
        if os.environ.get('HTTP_PROXY'):
            proxies = {
                'http': os.environ.get('HTTP_PROXY'),
                'https': os.environ.get('HTTPS_PROXY')
            }

        self.exchange = getattr(ccxt, EXCHANGE_ID)({
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}, # Default para dados de futuros
            # [NOVO] Força o uso do proxy definido nas variáveis de ambiente
            'proxies': proxies, 
            # [NOVO] Desativa a verificação SSL. Essencial para proxies corporativos/internos
            # que interceptam tráfego HTTPS, evitando erros de certificado.
            'verify': False 
        })

        self.state_file = os.path.join("configs", "bot_state.json")

        # Inicialização de variáveis de estado
        if not self._load_state():
            self.capital = initial_capital_usd
            self.position = None # Estrutura: {'symbol': str, 'size': float, 'entry_price': float, ...}
            self.accumulated_profit = 0.0
            self.accumulated_fees = 0.0
            self.peak_capital = initial_capital_usd
            self.pending_deposit_usd = 0.0
            self.next_funding_timestamp = None
            self._save_state()

    def _save_state(self):
        """
        [NOVO] Salva as variáveis críticas em um arquivo JSON.
        Deve ser chamado após qualquer alteração financeira ou de posição.
        """
        try:
            state = {
                'capital': self.capital,
                'position': self.position,
                'accumulated_profit': self.accumulated_profit,
                'accumulated_fees': self.accumulated_fees,
                'peak_capital': self.peak_capital,
                'pending_deposit_usd': self.pending_deposit_usd,
                'next_funding_timestamp': self.next_funding_timestamp
            }
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=4)
        except Exception as e:
            LOGGER.error(f"Erro ao salvar estado: {e}")

    def _load_state(self):
        """
        [NOVO] Tenta carregar o estado do arquivo JSON.
        Retorna True se sucesso, False se falha/arquivo não existe.
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
            self.peak_capital = state.get('peak_capital', 0.0)
            self.pending_deposit_usd = state.get('pending_deposit_usd', 0.0)
            self.next_funding_timestamp = state.get('next_funding_timestamp')
            
            LOGGER.info("Estado anterior carregado com SUCESSO.")
            if self.position:
                LOGGER.info(f"Retomando POS em: {self.position['symbol']}")
            
            return True
        except Exception as e:
            LOGGER.error(f"Erro ao carregar estado (arquivo corrompido?): {e}")
            return False

    def get_top_volume_pairs(self):
        """
        [ALTERADO] Escaneia o mercado usando Filtro Dinâmico de Consistência
        em vez de Whitelist fixa.
        """
        try:
            LOGGER.info("Iniciando varredura dinâmica de mercado...")
            tickers = self.exchange.fetch_tickers()
            
            # 1. Filtro Inicial: Volume Mínimo e Quote USDT
            # Expandimos o universo para qualquer par que tenha volume decente
            candidates = []
            
            for symbol, data in tickers.items():
                # Filtra apenas perpétuos USDT
                if '/USDT:USDT' in symbol and data['quoteVolume'] >= MIN_24H_VOLUME_USD:
                    candidates.append(symbol)
            
            LOGGER.info(f"Pre-filtro de volume: {len(candidates)} pares encontrados.")
            
            # 2. Filtro de Consistência de Funding (O Pulo do Gato)
            valid_pairs = []
            
            # Limitamos a analisar os top 20 por volume para não estourar API rate limit
            # Ordena por volume decrescente
            top_candidates = sorted(
                candidates, 
                key=lambda x: tickers[x]['quoteVolume'], 
                reverse=True
            )[:20]

            for symbol in top_candidates:
                if self._analyze_funding_consistency(symbol):
                    valid_pairs.append(symbol)
                    LOGGER.info(f"[APROVADO] filtro de consistência: {symbol}")
            
            return valid_pairs

        except Exception as e:
            LOGGER.error(f"Erro no scanner: {e}")
            return []

    def _analyze_funding_consistency(self, symbol):
        """
        [NOVO] Analisa se o histórico de funding é consistente e seguro.
        Regra: Média positiva nos últimos 3 dias e sem picos negativos graves.
        """
        try:
            # Busca histórico das últimas ~100 taxas (mas só usaremos as recentes)
            # Nota: Nem todas exchanges suportam histórico longo, mas Binance suporta bem.
            history = self.exchange.fetch_funding_rate_history(symbol, limit=20)
            
            if not history or len(history) < 9:
                return False # Dados insuficientes
            
            # Pega os últimos 9 pagamentos (aprox. 3 dias se for 8h/8h)
            recent_rates = [entry['fundingRate'] for entry in history[-9:]]
            
            # Critério 1: Média deve ser atrativa (> 0.01% por período)
            avg_rate = sum(recent_rates) / len(recent_rates)
            if avg_rate < 0.0001: 
                return False

            # Critério 2: Consistência (Nenhum negativo nos últimos 3 dias)
            # Isso evita moedas que oscilam demais
            if any(r < 0 for r in recent_rates):
                return False
                
            # Critério 3: Evitar "Armadilhas de Pump" (Opcional)
            # Se o último funding for 5x maior que a média, pode ser um pump artificial prestes a cair.
            current_rate = recent_rates[-1]
            if current_rate > (avg_rate * 5) and current_rate > 0.01:
                LOGGER.warning(f"Ignorado {symbol}: Pico suspeito de funding (Pump Risk).")
                return False

            return True

        except Exception as e:
            # Alguns pares podem dar erro ao buscar histórico
            return False

    def check_entry_opportunity(self, symbol):
        """
        Avalia viabilidade de entrada baseada em Funding e Payback de taxas.
        """
        try:
            funding_info = self.exchange.fetch_funding_rate(symbol)
            funding_rate = funding_info['fundingRate']
            
            # Filtro 1: Funding Positivo Mínimo
            if funding_rate <= MIN_FUNDING_RATE:
                # CORREÇÃO CRÍTICA: Alterado de 'return False' para 'return False, 0.0'.
                # O main.py espera desempacotar dois valores (is_viable, fr). Retornar apenas False causa o Crash.
                return False, funding_rate, "LOW_FUNDING"

            # Filtro 2: Payback das Taxas (Maker + Taker entrada e saída)
            # Custo total estimado (abertura + fechamento)
            total_fee_rate = (FEE_TAKER + FEE_MAKER) * 2 
            # Lucro projetado em 3 dias (3 funding payouts por dia * 3 dias)
            projected_return = funding_rate * 3 * DAYS_FOR_PAYBACK

            if projected_return > total_fee_rate:
                LOGGER.info(f"Oportunidade encontrada: {symbol} | FR: {funding_rate:.4%} | Proj. Retorno (3d): {projected_return:.4%}")
                return True, funding_rate, "SUCCESS"
            
            return False, funding_rate, "INSUFFICIENT_PAYBACK"

        except Exception as e:
            LOGGER.error(f"Erro ao verificar oportunidade para {symbol}: {e}")
            return False, 0.0, f"API_ERROR: {str(e)}"

    def simulate_entry(self, symbol, funding_rate):
        """
        Executa a lógica de entrada Delta-Neutro com simulação de slippage.
        Divide o capital 50/50 entre Spot e Futuros.
        """
        try:
            ticker_spot = self.exchange.fetch_ticker(symbol)
            price_spot_raw = ticker_spot['last']
            
            # Aplica Slippage na compra (Paga mais caro)
            entry_price_long = price_spot_raw * (1 + SLIPPAGE_SIMULATED)

            # Define alocação e quantidade baseada no preço Spot capturado
            allocation_per_leg = self.capital / 2
            quantity = allocation_per_leg / entry_price_long

            # 2. Simulação de "Execution Lag" (Latência)
            # Ocorre um atraso natural (rede, processamento da exchange) entre as ordens
            lag_seconds = random.uniform(0.5, 2.0) # Gera atraso entre 500ms e 2 segundos
            # LOGGER.info(f"Simulando latência de execução: {lag_seconds:.2f}s...") # Opcional: Descomentar para debug
            time.sleep(lag_seconds)

            # 3. Execução da Perna FUTURA (Atrasada)
            # Busca o preço novamente para refletir se o mercado se moveu durante o lag
            ticker_future = self.exchange.fetch_ticker(symbol)
            price_future_raw = ticker_future['last']

            # Aplica Slippage na venda Short (Vende mais barato)
            entry_price_short = price_future_raw * (1 - SLIPPAGE_SIMULATED)

            # Cálculo de Taxas
            # Taxa Spot + Taxa Futuros (Baseado no valor nocional de cada perna)
            cost_spot = (quantity * entry_price_long) * FEE_TAKER
            cost_future = (quantity * entry_price_short) * FEE_TAKER
            total_entry_fee = cost_spot + cost_future

            # Configuração do Funding
            funding_info = self.exchange.fetch_funding_rate(symbol)
            self.next_funding_timestamp = funding_info['nextFundingTimestamp'] / 1000
            
            self.position = {
                'symbol': symbol,
                'size': quantity,
                'entry_price_spot': entry_price_long,
                'entry_price_future': entry_price_short, # Preço pode ser diferente do Spot devido ao lag
                'current_funding_rate': funding_rate,
                'entry_time': time.time()
            }
            
            self.accumulated_fees += total_entry_fee
            self.capital -= total_entry_fee 
            
            # Log detalhado para auditoria de execução
            diff_price = entry_price_short - entry_price_long
            LOGGER.info(
                f"ENTRADA EXECUTADA ({symbol}):\n"
                f"   > Spot: ${entry_price_long:.2f} | Futuro: ${entry_price_short:.2f}\n"
                f"   > Lag: {lag_seconds:.2f}s | Spread Exec: {diff_price:.2f}\n"
                f"   > Qtd: {quantity:.4f} | Taxas Totais: ${total_entry_fee:.2f}"
            )
            
            self._save_state()
            return True

        except Exception as e:
            LOGGER.error(f"Erro na exec de entrada: {e}")
            return False

    def monitor_and_manage(self, db_manager):
        """
        Monitora a posição aberta: checa margem, realiza pagamento de funding e atualiza logs.
        """
        if not self.position:
            return

        symbol = self.position['symbol']
        try:
            # 1. Atualização de Dados de Mercado
            ticker = self.exchange.fetch_ticker(symbol)
            current_price = ticker['last']
            
            funding_info = self.exchange.fetch_funding_rate(symbol)
            current_funding = funding_info['fundingRate']
            
            # Obtém o timestamp do PRÓXIMO funding (em ms) informado pela API
            api_next_funding_ts = funding_info.get('nextFundingTimestamp')
            # Converte para segundos para comparação
            api_next_funding_sec = api_next_funding_ts / 1000 if api_next_funding_ts else None

            # 2. Lógica de Pagamento de Funding (Cash Flow Real) [NOVO]
            current_time = time.time()
            
            # Se temos um horário agendado e o tempo atual já passou dele:
            if self.next_funding_timestamp and current_time >= self.next_funding_timestamp:
                
                # Cálculo do Payout: Tamanho da Posição (em moedas) * Preço Atual * Taxa
                # No Cash&Carry (Short), se Funding > 0, nós RECEBEMOS.
                funding_payout = (self.position['size'] * current_price) * current_funding
                
                self.accumulated_profit += funding_payout
                
                LOGGER.info(f"FUNDING RECEBIDO: {symbol} | Valor: ${funding_payout:.4f} | Taxa: {current_funding:.4%}")
                
                # Atualiza o agendamento para o próximo ciclo (evita receber 2x no mesmo ms)
                # Usamos o dado fresco da API que já deve estar apontando para o futuro
                if api_next_funding_sec and api_next_funding_sec > current_time:
                    self.next_funding_timestamp = api_next_funding_sec
                else:
                    # Fallback de segurança: soma 8h se a API ainda não virou
                    self.next_funding_timestamp += 28800 

            # 3. Lógica de Segurança (Circuit Breaker)
            if current_funding < NEGATIVE_FUNDING_THRESHOLD:
                LOGGER.warning(f"CIRCUIT BREAKER: Funding negativo crit ({current_funding:.4%}). Saindo...")
                self._close_position(current_price, reason="Negative Funding")
                return

            # Checagem de Margem (Variação do preço contra o Short)
            price_change_pct = (current_price - self.position['entry_price_future']) / self.position['entry_price_future']
            
            if price_change_pct > (1 - SAFETY_MARGIN_RATIO): 
                LOGGER.warning("ALERTA: Margem pressionada. Preciso rebalanceamento.")

            # 4. Cálculo de PnL Não Realizado (Variação de Patrimônio)
            # Spot ganha na alta, Futuro (Short) perde na alta -> Tendem a zero
            spot_pnl = (current_price - self.position['entry_price_spot']) * self.position['size']
            future_pnl = (self.position['entry_price_future'] - current_price) * self.position['size']
            net_pnl_price = spot_pnl + future_pnl
            
            # Patrimônio Total = Capital Inicial + Lucro Realizado (Funding) + Variação Latente
            total_equity = self.capital + self.accumulated_profit + net_pnl_price
            drawdown = (self.peak_capital - total_equity) / self.peak_capital if self.peak_capital > 0 else 0
            
            # Formatação de data para o Log (apenas visual)
            next_funding_readable = datetime.fromtimestamp(self.next_funding_timestamp).strftime('%Y-%m-%d %H:%M:%S') if self.next_funding_timestamp else "N/A"

            # 5. Registro no Banco de Dados
            log_data = {
                'symbol': symbol,
                'price_spot': current_price,
                'price_future': current_price,
                'funding_rate': current_funding,
                'next_funding_time': next_funding_readable,
                'position_size': self.position['size'],
                'simulated_fees': self.accumulated_fees,
                # O lucro acumulado agora cresce a cada 8h
                'accumulated_profit': self.accumulated_profit + net_pnl_price,
                'max_drawdown': drawdown,
                'action': 'HOLD'
            }
            db_manager.log_state(log_data)
            
            # 6. Reinvestimento (Juros Compostos)
            self._process_compounding(current_price)

            self._save_state()

        except Exception as e:
            LOGGER.error(f"Erro no monitoramento: {e}")

    def _close_position(self, current_price, reason):
        """
        Fecha a posição, contabiliza taxas de saída e slippage.
        """
        exit_price_long = current_price * (1 - SLIPPAGE_SIMULATED)
        exit_price_short = current_price * (1 + SLIPPAGE_SIMULATED) # Compra short mais caro
        
        position_value = self.position['size'] * current_price
        exit_fee = (position_value * FEE_TAKER) * 2
        
        self.accumulated_fees += exit_fee
        self.capital -= exit_fee
        self.position = None
        LOGGER.info(f"POS CLOSED. Motivo: {reason} | Taxas Saida: ${exit_fee:.2f}")
        self._save_state()

    def deposit_monthly_contribution(self, exchange_rate=None):
        """
        Converte aporte em BRL para USD e adiciona ao saldo pendente.
        """
        # Se uma taxa específica não for passada, usa a constante do config
        rate_to_use = exchange_rate if exchange_rate else BRL_USD_RATE
        
        usd_amount = MONTHLY_CONTRIBUTION_BRL / rate_to_use
        self.pending_deposit_usd += usd_amount
        
        LOGGER.info(f"Aporte mensal registrado: R${MONTHLY_CONTRIBUTION_BRL:.2f} (Taxa: {rate_to_use:.2f}) -> ${usd_amount:.2f}")

    def _process_compounding(self, current_price):
        """
        Verifica se o saldo pendente permite aumentar a posição (Juros Compostos).
        Aplica cálculo de Preço Médio Ponderado para manter a precisão do PnL.
        """
        if self.pending_deposit_usd >= MIN_ORDER_VALUE_USD:
            # 1. Definição dos preços da nova tranche (com slippage simulado)
            # Mantém a coerência com a simulação de entrada original
            new_entry_spot = current_price * (1 + SLIPPAGE_SIMULATED)
            new_entry_future = current_price * (1 - SLIPPAGE_SIMULATED)

            # 2. Cálculo da nova quantidade baseada no capital disponível (50% por perna)
            allocation_per_leg = self.pending_deposit_usd / 2
            # Nota: Divide pelo preço real de execução (com slippage) para precisão do volume
            new_qty = allocation_per_leg / new_entry_spot

            # 3. Recuperação dos dados atuais da posição
            old_qty = self.position['size']
            old_price_spot = self.position['entry_price_spot']
            old_price_future = self.position['entry_price_future']
            
            total_new_qty = old_qty + new_qty

            # 4. Cálculo do Preço Médio Ponderado (Weighted Average Price)
            # Fórmula: ((Preço Antigo * Qtd Antiga) + (Preço Novo * Qtd Nova)) / Qtd Total
            avg_price_spot = ((old_price_spot * old_qty) + (new_entry_spot * new_qty)) / total_new_qty
            avg_price_future = ((old_price_future * old_qty) + (new_entry_future * new_qty)) / total_new_qty

            # 5. Atualização da Posição
            self.position['size'] = total_new_qty
            self.position['entry_price_spot'] = avg_price_spot
            self.position['entry_price_future'] = avg_price_future
            
            # Consome o depósito e atualiza capital contábil
            self.capital += self.pending_deposit_usd
            
            # Deduz taxas da nova entrada (Simulação de Taker)
            reinvest_fees = (allocation_per_leg * FEE_TAKER) * 2
            self.accumulated_fees += reinvest_fees
            self.capital -= reinvest_fees

            self.pending_deposit_usd = 0.0
            
            LOGGER.info(f"REINVESTIMENTO: +{new_qty:.4f} moedas. Novo Preço AVG Spot: ${avg_price_spot:.2f} | Futuro: ${avg_price_future:.2f}")