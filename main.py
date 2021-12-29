import json
import apache_beam as beam
from apache_beam import pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery

class TransformMessage(beam.DoFn):
    def process(self, element):
        element = element.decode('utf-8')
        element_json = json.loads(element)

        # print(element_json)
        return [element_json]


class CustomerNameChange(beam.DoFn):
    def process(self, element):
        name = element['customer_name'].split(" ")
        element['customer_first_name'] = name[0]
        element['customer_last_name'] = name[1]

        # print(element)
        return [element]


class CustomerAddress(beam.DoFn):
    def process(self, element):
        address_dict = {}
        address = element['order_address'].split(",")
        length_address = len('order_address')
        for k in (0, length_address):

            if k == 0:
                building_number = [int(s) for s in address[0].split() if s.isdigit()]
                address_dict['order_street_name'] = address[0]
            if building_number:
                address_dict['order_building_number'] = building_number[0]
            if k == 0:
                address_dict['order_city'] = address[1]
            if k == 0:
                state_code = address[k].split(" ")
                zipcode = [int(s) for s in address[k].split() if s.isdigit()]
                if state_code[1] is not None:
                    address_dict['order_state_code'] = state_code[1]
                if zipcode[0] is not None:
                    address_dict['order_zip_code'] = zipcode[0]

        #print(element)
        element['order_new_address'] = address_dict
        return [element]


class CostTotal(beam.DoFn):
    def process(self, element):
        price = 0
        for item in element['order_items']:
            price = item['price'] + price
        shipping = element['cost_shipping']
        tax = element['cost_tax']
        element['cost_total'] = float(price + shipping + tax)


        print(element)
        return [element]


table_schema = bigquery.TableSchema()

order_id_schema = bigquery.TableFieldSchema()
order_id_schema.name = 'order_id'
order_id_schema.type = 'integer'
order_id_schema.mode = 'nullable'
table_schema.fields.append(order_id_schema)

# A nested field
order_new_address_schema = bigquery.TableFieldSchema()
order_new_address_schema.name = 'order_new_address'
order_new_address_schema.type = 'record'
order_new_address_schema.mode = 'nullable'

order_building_number = bigquery.TableFieldSchema()
order_building_number.name = 'order_building_number'
order_building_number.type = 'string'
order_building_number.mode = 'nullable'
order_new_address_schema.fields.append(order_building_number)

order_street_name = bigquery.TableFieldSchema()
order_street_name.name = 'order_street_name'
order_street_name.type = 'string'
order_street_name.mode = 'nullable'
order_new_address_schema.fields.append(order_street_name)

order_city = bigquery.TableFieldSchema()
order_city.name = 'order_city'
order_city.type = 'string'
order_city.mode = 'nullable'
order_new_address_schema.fields.append(order_city)

order_state_code = bigquery.TableFieldSchema()
order_state_code.name = 'order_state_code'
order_state_code.type = 'string'
order_state_code.mode = 'nullable'
order_new_address_schema.fields.append(order_state_code)

order_zip_code = bigquery.TableFieldSchema()
order_zip_code.name = 'order_zip_code'
order_zip_code.type = 'string'
order_zip_code.mode = 'nullable'
order_new_address_schema.fields.append(order_zip_code)
table_schema.fields.append(order_new_address_schema)

customer_first_name_schema = bigquery.TableFieldSchema()
customer_first_name_schema.name = 'customer_first_name'
customer_first_name_schema.type = 'string'
customer_first_name_schema.mode = 'nullable'
table_schema.fields.append(customer_first_name_schema)

customer_last_name_schema = bigquery.TableFieldSchema()
customer_last_name_schema.name = 'customer_last_name'
customer_last_name_schema.type = 'string'
customer_last_name_schema.mode = 'nullable'
table_schema.fields.append(customer_last_name_schema)

customer_ip_schema = bigquery.TableFieldSchema()
customer_ip_schema.name = 'customer_ip'
customer_ip_schema.type = 'string'
customer_ip_schema.mode = 'nullable'
table_schema.fields.append(customer_ip_schema)

cost_total_schema = bigquery.TableFieldSchema()
cost_total_schema.name = 'cost_total'
cost_total_schema.type = 'float'
cost_total_schema.mode = 'nullable'
table_schema.fields.append(cost_total_schema)

order_currency_schema = bigquery.TableFieldSchema()
order_currency_schema.name = 'order_currency'
order_currency_schema.type = 'string'
order_currency_schema.mode = 'nullable'
table_schema.fields.append(order_currency_schema)


class module_bigquery(beam.DoFn):
    def process(self, element):
        print("module")
        columns_to_extract = ['order_id', 'order_new_address', 'customer_first_name', 'customer_last_name', 'customer_ip',
                              'cost_total', 'order_currency']
        new_set = {k: element[k] for k in columns_to_extract}

        return [new_set]


def run():
    pipeline_options = PipelineOptions(streaming=True)

    with beam.Pipeline(options=pipeline_options) as p:
        table_names = (p | "create input" >> beam.Create(
            [('USD', 'york-cdf-start:j_thomson_data_flow_proj_1.usd_order_payment_history'),
             ('EUR', 'york-cdf-start:j_thomson_data_flow_proj_1.eur_order_payment_history'),
             ('GBP', 'york-cdf-start:j_thomson_data_flow_proj_1.gbp_order_payment_history')]))
        table_names_dict = beam.pvalue.AsDict(table_names)
        messages = p | beam.io.ReadFromPubSub(
            subscription='projects/york-cdf-start/subscriptions/jt_test_topic-sub')
        first = messages | beam.ParDo(TransformMessage()) | beam.ParDo(CustomerNameChange())
        second = first | beam.ParDo(CustomerAddress()) | beam.ParDo(CostTotal())
        second | beam.ParDo(module_bigquery()) | 'write' >> beam.io.WriteToBigQuery(
            table=lambda row, table_dict: table_dict[row['order_currency']],
            table_side_inputs=(table_names_dict,),
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)





if __name__ == '__main__':
    run()
