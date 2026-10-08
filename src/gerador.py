import json
import time
from kafka import KafkaProducer
from faker import Faker
import random

# Configura o produtor para conectar no Kafka local e enviar dados em JSON
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

fake = Faker('pt_BR')

print("Iniciando envio de transações. Pressione Ctrl+C para parar.")

try:
    while True:
        # Gera dados falsos de uma transação
        transacao = {
            "id_transacao": fake.uuid4(),
            "data_hora": fake.iso8601(),
            "id_cliente": random.randint(1000, 9999),
            "valor": round(random.uniform(5.0, 2500.0), 2),
            "bandeira": random.choice(["Visa", "Mastercard", "Elo"]),
            "status": random.choice(["Aprovada", "Em análise", "Negada"])
        }
        
        # Envia para o tópico no Kafka
        producer.send('transacoes_bancarias', value=transacao)
        print(f"Enviado: {transacao['id_transacao']} | R$ {transacao['valor']}")
        
        # Aguarda 2 segundos antes da próxima transação
        time.sleep(2)
        
except KeyboardInterrupt:
    print("\nEnvio interrompido.")
finally:
    producer.close()