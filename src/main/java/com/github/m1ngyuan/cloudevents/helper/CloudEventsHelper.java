package com.github.m1ngyuan.cloudevents.helper;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventExtension;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.types.Time;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.Optional;
public class CloudEventsHelper {

    public static final String CE_ID = "Ce-Id";
    public static final String CE_TYPE = "Ce-Type";
    public static final String CE_SOURCE = "Ce-Source";
    public static final String CE_SPECVERSION = "Ce-Specversion";
    public static final String CE_TIME = "Ce-Time";
    public static final String CE_SUBJECT = "Ce-Subject";

    public static final String APPLICATION_JSON = "application/json";
    public static final String CONTENT_TYPE = "Content-Type";


    public static CloudEvent parseFromRequest(HttpHeaders headers, Object body) throws IllegalStateException {

        return parseFromRequestWithExtension(headers, body, null);
    }


    public static CloudEvent parseFromRequestWithExtension(HttpHeaders headers, Object body, CloudEventExtension extension) {
        if (headers.get(CE_ID) == null || (headers.get(CE_SOURCE) == null || headers.get(CE_TYPE) == null)) {
            throw new IllegalStateException("Cloud Event required fields are not present.");
        }
        Objects.requireNonNull(headers.get(CE_ID), "id fields are not present.");
        Objects.requireNonNull(headers.get(CE_SOURCE), "source fields are not present.");
        Objects.requireNonNull(headers.get(CE_TYPE), "type fields are not present.");

        String spec = headers.getFirst(CE_SPECVERSION);
        SpecVersion version = Objects.nonNull(spec) ? SpecVersion.parse(spec) : SpecVersion.V1;
        CloudEventBuilder builder = CloudEventBuilder
                .fromSpecVersion(version)
                .withDataContentType(Optional.ofNullable(headers.getFirst(CONTENT_TYPE)).orElse(APPLICATION_JSON));

        PropertyMapper mapper = PropertyMapper.get().alwaysApplyingWhenNonNull();
        mapper.from(headers.getFirst(CE_ID)).to(builder::withId);
        mapper.from(headers.getFirst(CE_TYPE)).to(builder::withType);
        mapper.from(headers.getFirst(CE_TIME)).to(time ->builder.withTime( Time.parseTime(time)));
        mapper.from(headers.getFirst(CE_SOURCE)).to(uri ->builder.withSource(URI.create(uri)));
        mapper.from(body).to(data ->builder.withData(body.toString().getBytes()));
        mapper.from(headers.getFirst(CE_SUBJECT)).to(builder::withSubject);
        mapper.from(extension).to(builder::withExtension);

        return builder.build();
    }

    public static WebClient.ResponseSpec createPostCloudEvent(WebClient webClient, String uriString, CloudEvent cloudEvent) {
        WebClient.RequestBodySpec uri = webClient.post().uri(uriString);
        WebClient.RequestHeadersSpec<?> headersSpec = uri.body(BodyInserters.fromValue(new String(cloudEvent.getData().toBytes())));
        WebClient.RequestHeadersSpec<?> header = headersSpec
                .header(CONTENT_TYPE, APPLICATION_JSON)
                .header(CE_TIME, Time.writeTime(Optional.ofNullable(cloudEvent.getTime()).orElse(OffsetDateTime.now())));

        PropertyMapper mapper = PropertyMapper.get().alwaysApplyingWhenNonNull();
        mapper.from(cloudEvent.getId()).to(v ->header.header(CE_ID,v));
        mapper.from(cloudEvent.getSpecVersion()).to(v ->header.header(CE_SPECVERSION,v.toString()));
        mapper.from(cloudEvent.getType()).to(v ->header.header(CE_TYPE,v));
        mapper.from(cloudEvent.getSubject()).to(v ->header.header(CE_SUBJECT,v));
        mapper.from(cloudEvent.getSource()).to(v ->header.header(CE_SOURCE,v.toString()));

        cloudEvent.getExtensionNames() //
                .stream() //
                .forEach(key -> //
                        mapper.from(cloudEvent.getExtension(key)).to(v ->header.header(key,v.toString()))
                );
        return header.retrieve();
    }


}
