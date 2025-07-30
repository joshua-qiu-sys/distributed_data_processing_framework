from random import choice
from decimal import Decimal
import datetime as dt
from schemas.consumer_good import ConsumerGood

class ConsumerGoodGenerator:
    def __init__(self):
        pass

    def generate_random_object_tuple(self):

        products = ['computer', 'television', 'smartphone', 'book', 'clothing', 'alarm clock', 'batteries', 'headphones',
                    'toothpaste', 'shampoo', 'laundry detergent', 'paper towels', 'charger', 'sunscreen', 'instant noodles',
                    'packaged snacks', 'light bulb', 'bottled water', 'air freshener', 'cooking oil', 'canned soup']
        retailers = ['Woolworths', 'Coles', 'Aldi', 'IGA', 'Amazon', 'Costco']
        prices = [Decimal('5.00'), Decimal('10.00'), Decimal('4.50'), Decimal('9.99'), Decimal('9.83'), Decimal('25.60'),
                Decimal('35.40'), Decimal('23.87'), Decimal('15.00'), Decimal('12.30'), Decimal('2.01'), Decimal('7.45'),
                Decimal('5.07'), Decimal('3.88'), Decimal('75.35'), Decimal('11.00'), Decimal('3.00'), Decimal('1.00'),
                Decimal('9.90'), Decimal('78.39'), Decimal('2.00')]
        txn_timestamp_delay = [0, 1, 2, 3, 4, 5, 10, 15]

        consumer_good_key = str(products.index(choice(products)))
        
        consumer_good_item = choice(products)
        consumer_good_retailer = choice(retailers)
        consumer_good_price = choice(prices)
        consumer_good_txn_timestamp = dt.datetime.now() - dt.timedelta(seconds=choice(txn_timestamp_delay))
        consumer_good = ConsumerGood(item=consumer_good_item, retailer=consumer_good_retailer, price=consumer_good_price, txn_timestamp=consumer_good_txn_timestamp)

        return (consumer_good_key, consumer_good)