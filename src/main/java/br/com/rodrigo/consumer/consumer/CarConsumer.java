package br.com.rodrigo.consumer.consumer;


import br.com.rodrigo.consumer.dto.CarDTO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class CarConsumer {

    private static final Logger logger = LoggerFactory.getLogger(CarConsumer.class);

    @Value(value = "${topic.name}")
    private String topic;

    @Value(value = "${spring.kafka.group-id}")
    private String groupId;

    //Estamos passando nessa anotação o topico a ser consultado, o grupo do consumer e o
    // containerFacotry que ciramos no arquivo de configuração KafkaConsumerConfig
    //A vantagem de usar ConsumerRecord no lugar de apenas CarDTO é que conseguimos ter dados do Broker
    @KafkaListener(topics = "${topic.name}", groupId = "${spring.kafka.group-id}", containerFactory = "carKafkaListenerContainerFactory")
    public void listenTopicCar(ConsumerRecord<String, CarDTO> mensagem){
        logger.info("A chave da mensagem é: "+mensagem.key());
        logger.info("A mensagem é: "+mensagem.value());
        logger.info("A partição da mensagem é: "+mensagem.partition());
    }
}
