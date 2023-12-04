package com.learnkafka.entity;

import jakarta.persistence.*;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.*;

@Getter
@Setter
@ToString(exclude = {"book"})
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
public class LibraryEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer libraryEventId;
    @Enumerated(EnumType.STRING)
    LibraryEventType libraryEventType;
    @Valid
    @NotNull
    @OneToOne(mappedBy = "libraryEvent", cascade = CascadeType.ALL)
    private Book book;

}
