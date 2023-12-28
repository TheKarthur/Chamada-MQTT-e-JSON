import json
from paho.mqtt import client as mqtt_client

# Parâmetros de conexão TCP
host = 'test.mosquitto.org'
port = 1883
topic = 'Liberato/iotTro/44xx/ack/20000277'
client_id = 'ARTHUR'

# Função para verificar se uma string é um JSON válido
def is_json(string):
    try:
        json.loads(string)
    except ValueError as e:
        return False
    return True

# Função para estabelecer conexão com o broker MQTT
def connect_mqtt():
    # Função de callback chamada quando a conexão com o broker é estabelecida
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Conexão estabelecida com sucesso!")
        else:
            print("FALHA NA CONEXÃO, ERRO: %d\n", rc)

    # Cria o objeto do cliente MQTT
    client = mqtt_client.Client(client_id)

    # Atribui a função de callback criada ao objeto
    client.on_connect = on_connect

    # Realiza a conexão com o broker
    client.connect(host, port)

    return client

# Função para fazer uma inscrição em um tópico do broker
def subscribe(client: mqtt_client):
    # Função de callback chamada quando uma mensagem é recebida
    def on_message(client, userdata, msg):
        print(msg.payload.decode())
    # Efetua a inscrição no tópico
    client.subscribe(topic)
    # Define a função de callback a ser utilizada
    client.on_message = on_message


if __name__ == '__main__':
    # Cria um objeto de cliente MQTT
    client = connect_mqtt()
    # Realiza a inscrição
    subscribe(client)
    # Mantém o loop para receber mensagens
    client.loop_forever()
