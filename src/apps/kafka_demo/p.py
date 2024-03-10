from kafkadriver import kafkaConnector

prod = kafkaConnector()
prod.start_producer()
data = {'load' : [0.0, 0.3, 0.4, 0.5, 0.1, 0.0], 'radiance' : [0.0, 0.0, 123.3, 222.1, 22.3, 0]}
prod.send_message('classification', data)
