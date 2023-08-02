

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from tvDatafeed import TvDatafeed, Interval


bitcoin_rpc_user = '****'
bitcoin_rpc_password = '****'
bitcoin_rpc_port = 8332

influxdb_url = 'http://localhost:8086'
influxdb_token = '****'
influxdb_org = 'test'
influxdb_bucket = 'test'

start_block = 0

bitcoin_rpc_connection = AuthServiceProxy(f'http://{bitcoin_rpc_user}:{bitcoin_rpc_password}@localhost:{bitcoin_rpc_port}')

influxdb_client = InfluxDBClient(url=influxdb_url, token=influxdb_token)
influxdb_write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)

block_height = bitcoin_rpc_connection.getblockcount()

for block_number in range(start_block, block_height + 1):
    block_hash = bitcoin_rpc_connection.getblockhash(block_number)
    block = bitcoin_rpc_connection.getblock(block_hash)

#    for k, v in block.items():
#        print (k)
    timestamp = block['time']
    nanos = int(timestamp) * 10**9

    difficulty = float(block['difficulty'])
    transaction_count = len(block['tx'])

    point = Point('block').tag('source', 'full-node').field('block_number', block_number).time(nanos)
    influxdb_write_api.write(bucket=influxdb_bucket, record=point, org=influxdb_org)

    point = Point('block').tag('source', 'full-node').field('difficulty', difficulty).time(nanos)
    influxdb_write_api.write(bucket=influxdb_bucket, record=point, org=influxdb_org)

    point = Point('block').tag('source', 'full-node').field('transaction_count', transaction_count).time(nanos)
    influxdb_write_api.write(bucket=influxdb_bucket, record=point, org=influxdb_org)

    print('Imported block {}/{} into InfluxDB.'.format(block_number, block_height))

print('Blockchain import completed.')

tv = TvDatafeed()

freq_list = [
    Interval.in_1_minute,
    Interval.in_3_minute,
    Interval.in_5_minute,
    Interval.in_15_minute,
    Interval.in_30_minute,
    Interval.in_45_minute,
    Interval.in_1_hour,
    Interval.in_2_hour,
    Interval.in_3_hour,
    Interval.in_4_hour,
    Interval.in_daily,
    Interval.in_weekly,
    Interval.in_monthly,
]

symbols_bitstamp = ['BTCUSD']
for symbol in symbols_bitstamp:
    for freq in freq_list:
        df = tv.get_hist(symbol, 'BITSTAMP', interval=freq, n_bars=5000)
        freq = str(freq)
        freq = freq[freq.index('_') + 1:]
        print ('Importing bitstamp price: {} freq: {} into InfluxDB...'.format(symbol, freq))
        df['datetime'] = df.index
        for i in range(len(df)):
            nanos = df['datetime'].iloc[i].value
            point = Point('price').tag('source', 'tradingview').tag('exchange', 'bitstamp').tag('ticker', symbol).tag('freq', freq).field('open', df['open'].iloc[i]).time(nanos)
            influxdb_write_api.write(bucket=influxdb_bucket, record=point, org=influxdb_org)
            point = Point('price').tag('source', 'tradingview').tag('exchange', 'bitstamp').tag('ticker', symbol).tag('freq', freq).field('high', df['high'].iloc[i]).time(nanos)
            influxdb_write_api.write(bucket=influxdb_bucket, record=point, org=influxdb_org)
            point = Point('price').tag('source', 'tradingview').tag('exchange', 'bitstamp').tag('ticker', symbol).tag('freq', freq).field('low', df['low'].iloc[i]).time(nanos)
            influxdb_write_api.write(bucket=influxdb_bucket, record=point, org=influxdb_org)
            point = Point('price').tag('source', 'tradingview').tag('exchange', 'bitstamp').tag('ticker', symbol).tag('freq', freq).field('close', df['close'].iloc[i]).time(nanos)
            influxdb_write_api.write(bucket=influxdb_bucket, record=point, org=influxdb_org)
            point = Point('price').tag('source', 'tradingview').tag('exchange', 'bitstamp').tag('ticker', symbol).tag('freq', freq).field('volume', df['volume'].iloc[i]).time(nanos)
            influxdb_write_api.write(bucket=influxdb_bucket, record=point, org=influxdb_org)

print ('TradingView bitstamp price import completed.')

symbols_cryptocap = ['BTC', 'BTC.D']
for symbol in symbols_cryptocap:
    for freq in freq_list:
        df = tv.get_hist(symbol, 'CRYPTOCAP', interval=freq, n_bars=5000)
        freq = str(freq)
        freq = freq[freq.index('_') + 1:]
        print ('Importing cryptocap metric: {} freq: {} into InfluxDB...'.format(symbol, freq))
        df['datetime'] = df.index
        for i in range(len(df)):
            nanos = df['datetime'].iloc[i].value
            point = Point('metric').tag('source', 'tradingview').tag('exchange', 'cryptocap').tag('ticker', symbol).tag('freq', freq).field('open', df['open'].iloc[i]).time(nanos)
            influxdb_write_api.write(bucket=influxdb_bucket, record=point, org=influxdb_org)
            point = Point('metric').tag('source', 'tradingview').tag('exchange', 'cryptocap').tag('ticker', symbol).tag('freq', freq).field('high', df['high'].iloc[i]).time(nanos)
            influxdb_write_api.write(bucket=influxdb_bucket, record=point, org=influxdb_org)
            point = Point('metric').tag('source', 'tradingview').tag('exchange', 'cryptocap').tag('ticker', symbol).tag('freq', freq).field('low', df['low'].iloc[i]).time(nanos)
            influxdb_write_api.write(bucket=influxdb_bucket, record=point, org=influxdb_org)
            point = Point('metric').tag('source', 'tradingview').tag('exchange', 'cryptocap').tag('ticker', symbol).tag('freq', freq).field('close', df['close'].iloc[i]).time(nanos)
            influxdb_write_api.write(bucket=influxdb_bucket, record=point, org=influxdb_org)
            point = Point('metric').tag('source', 'tradingview').tag('exchange', 'cryptocap').tag('ticker', symbol).tag('freq', freq).field('volume', df['volume'].iloc[i]).time(nanos)
            influxdb_write_api.write(bucket=influxdb_bucket, record=point, org=influxdb_org)

print ('TradingView cryptocap metric import completed.')
