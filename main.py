import paho.mqtt.client as mqtt
import json

# Número de matrícula do dispositivo
matricula = 20000277

# Broker MQTT e tópico de inscrição
broker_address = "test.mosquitto.org"
broker_port = 1883
data_topic = "Liberato/iotTro/44xx/data"
reply_topic = f"Liberato/iotTro/44xx/rply/{matricula}"
error_topic = "Liberato/iotTro/44xx/ack/"


# Função chamada quando o cliente MQTT se conecta ao broker
def on_connect(client, userdata, flags, rc):
    print(f"Conectado ao Broker com código de resultado: {rc}")
    client.subscribe(data_topic)


# Função chamada quando o cliente MQTT recebe uma mensagem
def on_message(client, userdata, message):
    try:
        payload = json.loads(message.payload.decode("utf-8"))
        print("Mensagem Recebida:")
        print(json.dumps(payload, indent=2))

        # Verifica se a matrícula na mensagem corresponde à matrícula do dispositivo
        if "matricula" in payload and payload["matricula"] == matricula:
            # Modifica os campos da mensagem
            payload["seq"] += 800000
            if float(payload["tempInt"].get("valor")) < float(payload["tempExt"].get("valor")):
                payload["climatizado"] = "SIM"
            else:
                payload["climatizado"] = "NAO"
            payload["nome"] = "Arthur Rodrigues Padilha"
            payload["turma"] = "4411"

            # Remove campos de temperatura e umidade
            payload.pop("temperatura", None)
            payload.pop("umidade", None)
            payload.pop("tempExt", None)
            payload.pop("tempInt", None)

            print("JSON Modificado:")
            print(json.dumps(payload, indent=2))

            # Envia a mensagem modificada para o tópico de resposta
            client.publish(reply_topic, json.dumps(payload))
        else:
            print("Matrícula não correspondente. Ignorando a mensagem.")

    except Exception as e:
        print(f"Erro ao processar a mensagem: {e}")


# Cria o cliente MQTT e configura as funções de conexão e recepção de mensagens
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Conecta-se ao broker MQTT e mantém o loop para receber mensagens
client.connect(broker_address, broker_port, 60)
client.loop_forever()
