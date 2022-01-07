package com.example.demo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
public class JobConfig {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;


    @Bean
    public Job sampleJob() {
        return jobBuilderFactory.get("sampleJob")
            .start(sampleStep())
            .build();
    }

    @Bean
    @JobScope
    public Step sampleStep() {
        return stepBuilderFactory.get("sampleStep")
            .chunk(2)
            .reader(playerFlatFileItemReader())
            .writer(System.out::println)
            .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Player> playerFlatFileItemReader() {
        FlatFileItemReader<Player> itemReader = new FlatFileItemReader<>();
        ClassPathResource resource = new ClassPathResource("player.csv");
        itemReader.setResource(resource);
        DefaultLineMapper<Player> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(new DelimitedLineTokenizer());
        PlayerFieldSetMapper fieldSetMapper = new PlayerFieldSetMapper();
        lineMapper.setFieldSetMapper(fieldSetMapper);
        itemReader.setLineMapper(lineMapper);
        itemReader.setLinesToSkip(1);
        return itemReader;
    }

    protected static class PlayerFieldSetMapper implements FieldSetMapper<Player> {
        public Player mapFieldSet(FieldSet fieldSet) {
            String[] split = fieldSet.readString(0).split(",");
            Player player = new Player();
            player.setID(split[0]);
            player.setLastName(split[1]);
            player.setFirstName(split[2]);
            player.setPosition(split[3]);
            player.setBirthYear(Integer.parseInt(split[4]));
            player.setDebutYear(Integer.parseInt(split[5]));
            return player;
        }
    }
}
