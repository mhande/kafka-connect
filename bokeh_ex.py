import random
import json
from bokeh.driving import count
from bokeh.models import ColumnDataSource
from bokeh.layouts import column,row,gridplot
from bokeh.models.annotations import LabelSet

from bokeh.models import ColumnDataSource, DataRange1d, Plot, LinearAxis, Grid, Legend, LegendItem
from bokeh.core.properties import Color
from bokeh.plotting import curdoc, figure

from kafka import KafkaConsumer, TopicPartition

UPDATE_INTERVAL = 100
ROLLOVER = 100 # Number of displayed data points
servers = ['192.168.99.100:9092']

city_amount = dict()
city_units = dict()

source = ColumnDataSource({"x": [''], "y": ['']})
source1 = ColumnDataSource({"x": [''], "y": ['']})
source2 = ColumnDataSource({"x": [''], "y": ['']})
source3 = ColumnDataSource({"x": [''], "y": ['']})
source4 = ColumnDataSource(data=dict(x=['Chennai', 'Hyderabad', 'Kadapa', 'Bangalore'], y=[0,0,0,0]))
source5 = ColumnDataSource(data=dict(x=['Chennai', 'Hyderabad', 'Kadapa', 'Bangalore'], y=[0,0,0,0]))

consumer = KafkaConsumer(bootstrap_servers=servers,
                         value_deserializer=lambda m: json.loads(m).encode('ascii'),
                         group_id='chartjs',
                         auto_offset_reset="latest")
                         
consumer.subscribe(['orders'])

@count()
def update(x):
    y = random.random()
    msg = next(consumer)
    data = msg.value
    print(msg)
    city_name = json.loads(data).get('city')
    city_amount[city_name] = city_amount.get(city_name, 0) + json.loads(data).get('order_total')
    source.stream({"x": [x], "y": [city_amount['Hyderabad']]}, rollover=ROLLOVER)
    source1.stream({"x": [x], "y": [city_amount['Bangalore']]}, rollover=ROLLOVER)
    source2.stream({"x": [x], "y": [city_amount['Kadapa']]}, rollover=ROLLOVER)
    source3.stream({"x": [x], "y": [city_amount['Chennai']]}, rollover=ROLLOVER)
    values = [city_amount.get('Chennai', 0),city_amount.get('Hyderabad', 0),city_amount.get('Kadapa', 0),city_amount.get('Bangalore', 0)]
    
    
    source4.stream({"x": ['Chennai', 'Hyderabad', 'Kadapa', 'Bangalore'], "y":values})
    
    items =  json.loads(data).get('items')
    total_units = sum([item.get('total_units') for item in items if item.get('name').lower() == 'onion'])
    city_units[city_name] = city_units.get(city_name, 0) + total_units
    item_values = [city_units.get('Chennai', 0),city_units.get('Hyderabad', 0),city_units.get('Kadapa', 0),city_units.get('Bangalore', 0)]
    
    source5.stream({"x": ['Chennai', 'Hyderabad', 'Kadapa', 'Bangalore'], "y":item_values})

p = figure(height=300,width=600, title="City v/s order_total Graph")
p.line("x", "y", source=source, line_color="blue", legend_label="Hyderabad")
p.line("x", "y", source=source1, line_color="red", legend_label="Bangalore")
p.line("x", "y", source=source2, line_color="black", legend_label="Kadapa")
p.line("x", "y", source=source3, legend_label="Chennai")
p.legend.location='top_left'


b = figure(x_range=['Chennai', 'Hyderabad', 'Kadapa', 'Bangalore'],height=300,width=600, title="Bar Chart: City v/s order_total Graph")
            
b.vbar("x", top="y",width=0.5, source=source4,fill_color="#b3de69")

b.xgrid.grid_line_color = None
b.y_range.start = 0




o = figure(title='City v/s Onion Consumption', x_range=['Chennai', 'Hyderabad', 'Kadapa', 'Bangalore'],height=300,width=600)

o.vbar("x", top="y",width=0.5, source=source5)

o.xgrid.grid_line_color = None
o.y_range.start = 0


c = gridplot([[p, b], [o]])

doc = curdoc()
doc.add_root(c)
doc.add_periodic_callback(update, UPDATE_INTERVAL)