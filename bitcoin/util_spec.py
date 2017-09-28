from util import gdax_time_parser

assert str(gdax_time_parser('2017-09-26T04:40:51.596000Z')) == '2017-09-26 04:40:51.596000+00:00'