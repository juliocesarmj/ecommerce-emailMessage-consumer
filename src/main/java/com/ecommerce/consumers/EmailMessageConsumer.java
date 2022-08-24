package com.ecommerce.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import com.ecommerce.dtos.EmailMessageDto;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class EmailMessageConsumer {
	
	@Value("${spring.mail.username}")
	private String from;
	
	private final JavaMailSender javaMailSender;
	
	@KafkaListener(topics = "${topic.name.consumer}", groupId = "group_id")
	public void consume(ConsumerRecord<String, String> record) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		
		EmailMessageDto dto = mapper.readValue(record.value(), EmailMessageDto.class);
		
		SimpleMailMessage message = new SimpleMailMessage();
		message.setFrom(this.from);
		message.setTo(dto.getTo());
		message.setSubject(dto.getSubject());
		message.setText(dto.getBody());
		
		this.javaMailSender.send(message);
	}

}
